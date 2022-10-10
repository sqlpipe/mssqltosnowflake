package main

import (
	"net/http"
	"time"

	"github.com/sqlpipe/mssqltosnowflake/internal/data"
	"github.com/sqlpipe/mssqltosnowflake/internal/validator"
)

func (app *application) createTransferHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
		AwsKey                   string `json:"aws_key"`
		AwsSecret                string `json:"aws_secret"`
		AwsToken                 string `json:"aws_token"`
		AwsS3Bucket              string `json:"aws_s3_bucket"`
		AwsS3Dir                 string `json:"aws_s3_dir"`
		SourceHost               string `json:"source_host"`
		SourcePort               int    `json:"source_port"`
		SourceUsername           string `json:"source_username"`
		SourcePassword           string `json:"source_password"`
		SourceDb                 string `json:"source_db"`
		TargetAccountId          string `json:"target_account_id"`
		TargetPrivateKeyLocation string `json:"target_private_key_location"`
		TargetRole               string `json:"target_role"`
		TargetWarehouse          string `json:"target_warehouse"`
		TargetAwsRegion          string `json:"target_aws_region"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.badRequestResponse(w, r, err)
		return
	}

	v := validator.New()

	aws := &data.Aws{
		Key:      input.AwsKey,
		Secret:   input.AwsSecret,
		Token:    input.AwsToken,
		S3Bucket: input.AwsS3Bucket,
		S3Dir:    input.AwsS3Dir,
	}

	source := &data.Source{
		Host:     input.SourceHost,
		Port:     input.SourcePort,
		Username: input.SourceUsername,
		Password: input.SourcePassword,
		Db:       input.SourcePassword,
	}

	target := &data.Target{
		AccountId:  input.TargetAccountId,
		PrivateKey: input.TargetPrivateKey,
		Role:       input.TargetRole,
		Warehouse:  input.TargetWarehouse,
		AwsRegion:  input.TargetAwsRegion,
	}

	data.ValidateAws(v, aws)
	data.ValidateSource(v, source)
	data.ValidateTarget(v, target)

	if !v.Valid() {
		app.failedValidationResponse(w, r, v.Errors)
		return
	}

	transferId, err := randomCharacters(32)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}

	transfer := &data.Transfer{
		Id:        transferId,
		CreatedAt: time.Now(),
		Source:    *source,
		Target:    *target,
		Aws:       *aws,
	}

	headers := make(http.Header)

	err = app.writeJSON(w, http.StatusCreated, envelope{"transfer": transfer}, headers)
	if err != nil {
		app.serverErrorResponse(w, r, err)
	}
}
