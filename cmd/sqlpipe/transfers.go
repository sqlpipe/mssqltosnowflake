package main

import (
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/snowflakedb/gosnowflake"

	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sqlpipe/mssqltosnowflake/internal/data"
	"github.com/sqlpipe/mssqltosnowflake/internal/validator"
	"github.com/sqlpipe/mssqltosnowflake/pkg"
)

func (app *application) createTransferHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		AwsConfigKey             string `json:"aws_config_key"`
		AwsConfigSecret          string `json:"aws_config_secret"`
		AwsConfigToken           string `json:"aws_config_token"`
		AwsConfigS3Bucket        string `json:"aws_config_s3_bucket"`
		AwsConfigS3Dir           string `json:"aws_config_s3_dir"`
		AwsConfigRegion          string `json:"aws_config_region"`
		SourceHost               string `json:"source_host"`
		SourcePort               int    `json:"source_port"`
		SourceUsername           string `json:"source_username"`
		SourcePassword           string `json:"source_password"`
		SourceDbName             string `json:"source_db_name"`
		TargetAccountId          string `json:"target_account_id"`
		TargetUsername           string `json:"target_username"`
		TargetPrivateKeyLocation string `json:"target_private_key_location"`
		TargetRole               string `json:"target_role"`
		TargetWarehouse          string `json:"target_warehouse"`
		TargetAwsRegion          string `json:"target_aws_region"`
		TargetDbName             string `json:"target_db_name"`
		TargetFileFormatName     string `json:"target_file_format_name"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	v := validator.New()

	awsConfig := data.AwsConfig{
		Key:      input.AwsConfigKey,
		Secret:   input.AwsConfigSecret,
		Token:    input.AwsConfigToken,
		S3Bucket: input.AwsConfigS3Bucket,
		S3Dir:    input.AwsConfigS3Dir,
		Region:   input.AwsConfigRegion,
	}

	source := data.Source{
		Host:     input.SourceHost,
		Port:     input.SourcePort,
		Username: input.SourceUsername,
		Password: input.SourcePassword,
		DbName:   input.SourceDbName,
	}

	target := data.Target{
		AccountId:          input.TargetAccountId,
		PrivateKeyLocation: input.TargetPrivateKeyLocation,
		Role:               input.TargetRole,
		Warehouse:          input.TargetWarehouse,
		AwsRegion:          input.TargetAwsRegion,
		Username:           input.TargetUsername,
		DbName:             input.TargetDbName,
		FileFormatName:     input.TargetFileFormatName,
	}

	data.ValidateAwsConfig(v, awsConfig)
	data.ValidateSource(v, source)
	data.ValidateTarget(v, target)

	if !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	query := url.Values{}
	query.Add("database", source.DbName)

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(source.Username, source.Password),
		Host:     fmt.Sprintf("%s:%d", source.Host, source.Port),
		RawQuery: query.Encode(),
	}
	sourceDsn := u.String()

	sourceDb, err := sql.Open("mssql", sourceDsn)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	source.Db = *sourceDb

	creds := credentials.NewStaticCredentialsProvider(
		awsConfig.Key,
		awsConfig.Secret,
		awsConfig.Token,
	)

	awsClientCfg, err := config.LoadDefaultConfig(
		r.Context(),
		config.WithRegion(awsConfig.Region),
		config.WithCredentialsProvider(creds),
	)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	s3Client := s3.NewFromConfig(awsClientCfg)

	awsConfig.Uploader = *manager.NewUploader(s3Client)

	priv, err := ioutil.ReadFile(target.PrivateKeyLocation)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}
	privPem, _ := pem.Decode(priv)
	if len(privPem.Bytes) == 0 {
		app.serverErrorResponse(w, r, errors.New("unable to read private key pem bytes"))
		return
	}
	privPemBytes := privPem.Bytes
	var parsedKey interface{}
	if parsedKey, err = x509.ParsePKCS1PrivateKey(privPemBytes); err != nil {
		if parsedKey, err = x509.ParsePKCS8PrivateKey(privPemBytes); err != nil {
			app.serverErrorResponse(w, r, err)
			return
		}
	}
	privKey, ok := parsedKey.(*rsa.PrivateKey)
	if !ok {
		app.serverErrorResponse(w, r, err)
		return
	}

	target.PrivateKey = *privKey

	snowflakeConfig := gosnowflake.Config{
		Account:       target.AccountId,
		User:          target.Username,
		Database:      target.DbName,
		Warehouse:     target.Warehouse,
		Role:          target.Role,
		Authenticator: gosnowflake.AuthTypeJwt,
		PrivateKey:    &target.PrivateKey,
	}

	targetDsn, err := gosnowflake.DSN(&snowflakeConfig)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	targetDb, err := sql.Open("snowflake", targetDsn)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	target.Db = *targetDb

	transferId, err := pkg.RandomCharacters(32)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}

	transfer := data.Transfer{
		Id:        transferId,
		CreatedAt: time.Now(),
		Source:    source,
		Target:    target,
		AwsConfig: awsConfig,
	}

	err = transfer.Run(r.Context())
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}

	headers := make(http.Header)

	err = app.writeJSON(w, http.StatusCreated, envelope{"transfer": transfer}, headers)
	if err != nil {
		app.serverErrorResponse(w, r, err)
		return
	}
}
