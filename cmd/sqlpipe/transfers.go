package main

import (
	"reflect"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/snowflakedb/gosnowflake"
	"golang.org/x/sync/errgroup"

	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/csv"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/sqlpipe/mssqltosnowflake/internal/data"
	"github.com/sqlpipe/mssqltosnowflake/internal/validator"
	"github.com/sqlpipe/mssqltosnowflake/pkg"
)

func (app *application) showTransferHandler(w http.ResponseWriter, r *http.Request) {
	qs := r.URL.Query()

	id := app.readString(qs, "id", "")

	transfer, ok := app.transferMap[id]
	if !ok {
		app.notFoundResponse(w, r)
		return
	}

	err := app.writeJSON(w, http.StatusOK, envelope{"transfer": transfer}, nil)
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, err)
	}
}

func (app *application) createTransferHandler(w http.ResponseWriter, r *http.Request) {
	var input struct {
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
		TargetStorageIntegration string `json:"target_storage_integration"`
		TargetDivisionCode       string `json:"target_division_code"`
		TargetFileFormatName     string `json:"target_file_format_name"`
		// ServerName               string `json:"server_name"`
	}

	err := app.readJSON(w, r, &input)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, fmt.Sprintf("unable to read JSON, err: %v", err))
		return
	}

	v := validator.New()

	awsConfig := data.AwsConfig{
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

	// remove .NA.PACCAR.COM from servername
	// serverName := strings.Replace(input.ServerName, ".NA.PACCAR.COM", "", -1)

	target := data.Target{
		AccountId:          input.TargetAccountId,
		PrivateKeyLocation: input.TargetPrivateKeyLocation,
		Role:               input.TargetRole,
		Warehouse:          input.TargetWarehouse,
		AwsRegion:          input.TargetAwsRegion,
		Username:           input.TargetUsername,
		DbName:             input.TargetDbName,
		StorageIntegration: input.TargetStorageIntegration,
		DivisionCode:       input.TargetDivisionCode,
		// ServerName:         serverName,
		// FileFormatName:     input.TargetFileFormatName,
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
		app.errorResponse(w, r, http.StatusBadRequest, fmt.Sprintf("unable to open source db, err: %v", err))
		return
	}

	source.Db = *sourceDb

	priv, err := ioutil.ReadFile(target.PrivateKeyLocation)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, fmt.Sprintf("unable to read private key file, err: %v", err))
		return
	}
	privPem, _ := pem.Decode(priv)
	if len(privPem.Bytes) == 0 {
		app.errorResponse(w, r, http.StatusBadRequest, fmt.Sprintf("unable to read private key pem bytes, err: %v", err))
		return
	}
	privPemBytes := privPem.Bytes
	var parsedKey interface{}
	if parsedKey, err = x509.ParsePKCS1PrivateKey(privPemBytes); err != nil {
		if parsedKey, err = x509.ParsePKCS8PrivateKey(privPemBytes); err != nil {
			app.errorResponse(w, r, http.StatusBadRequest, fmt.Sprintf("unable to parse private key pem bytes, err: %v", err))
			return
		}
	}
	privKey, ok := parsedKey.(*rsa.PrivateKey)
	if !ok {
		app.errorResponse(w, r, http.StatusBadRequest, fmt.Sprintf("unable to assert privkey to *rsa.PrivateKey, err: %v", err))
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
		// Schema:        "sqlpipe",
	}

	targetDsn, err := gosnowflake.DSN(&snowflakeConfig)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, fmt.Sprintf("unable to construct a snowflake DSN, err: %v", err))
		return
	}

	targetDb, err := sql.Open("snowflake", targetDsn)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, fmt.Sprintf("unable to open a connection to snowflake, err: %v", err))
		return
	}

	target.Db = *targetDb

	transferId, err := pkg.RandomCharacters(32)
	if err != nil {
		app.errorResponse(w, r, http.StatusInternalServerError, fmt.Sprintf("unable to generate random characters, err: %v", err))
		return
	}

	transfer := data.Transfer{
		Id:        transferId,
		CreatedAt: time.Now(),
		Source:    source,
		Target:    target,
		AwsConfig: awsConfig,
		Status:    "running",
	}

	app.transferMap[transferId] = transfer

	headers := make(http.Header)

	responseMessage := envelope{
		"transfer_id": transfer.Id,
		"status":      "running",
		"error":       "",
	}

	app.transferMap[transfer.Id] = transfer

	err = app.writeJSON(w, http.StatusOK, responseMessage, headers)
	if err != nil {
		app.errorResponse(w, r, http.StatusBadRequest, fmt.Sprintf("error writing json response, err: %v", err))
		return
	}

	app.background(func() {
		err = app.Run(transfer)
		if err != nil {
			transfer.Status = "failed"
			transfer.Error = err.Error()
			app.transferMap[transfer.Id] = transfer
			return
		}

		transfer.Status = "complete"
		app.transferMap[transfer.Id] = transfer
	})
}

func (app *application) Run(transfer data.Transfer) error {
	schemaRows, err := transfer.Source.Db.Query(
		"SELECT S.name as schema_name, T.name as table_name FROM sys.tables AS T INNER JOIN sys.schemas AS S ON S.schema_id = T.schema_id LEFT JOIN sys.extended_properties AS EP ON EP.major_id = T.[object_id] WHERE T.is_ms_shipped = 0 AND (EP.class_desc IS NULL OR (EP.class_desc <>'OBJECT_OR_COLUMN' AND EP.[name] <> 'microsoft_database_tools_support'))",
	)
	if err != nil {
		return fmt.Errorf("error running query getting all db objects: %v", err)
	}
	defer schemaRows.Close()

	var sourceSchema string
	var sourceTable string
	queries := []data.Query{}
	for schemaRows.Next() {
		err := schemaRows.Scan(&sourceSchema, &sourceTable)
		if err != nil {
			return fmt.Errorf("error scanning schema and table into query object: %v", err)
		}

		sourceQuery := fmt.Sprintf("select * from [%v].[%v]", sourceSchema, sourceTable)

		query := data.Query{
			Schema:      sourceSchema,
			Table:       sourceTable,
			SourceQuery: sourceQuery,
		}

		queries = append(queries, query)
	}
	err = schemaRows.Err()
	if err != nil {
		return fmt.Errorf("error iterating over schemaRows: %v", err)
	}

	transfer.Queries = queries
	sourceDbNameHasNonAlnum := data.HasNonAlnum(transfer.Source.DbName)
	// [Division]_MSSQL_[Source]_[Server]
	schemaNameInSnowflake := data.QuoteIfTrue(
		fmt.Sprintf(
			`%v_MSSQL_%v`,
			strings.ToUpper(transfer.Target.DivisionCode),
			strings.ToUpper(transfer.Source.DbName),
			// strings.ToUpper(transfer.Target.ServerName),
		),
		sourceDbNameHasNonAlnum,
	)

	// remove .NA.PACCAR.COM from schema name
	schemaNameInSnowflake = strings.ReplaceAll(schemaNameInSnowflake, ".NA.PACCAR.COM", "")

	dropSchemaQuery := fmt.Sprintf(
		`drop schema if exists %v`,
		schemaNameInSnowflake,
	)
	_, err = transfer.Target.Db.Exec(
		dropSchemaQuery,
	)
	if err != nil {
		return fmt.Errorf("error running drop schema query, query was %v. error was: %v", dropSchemaQuery, err)
	}

	createSchemaQuery := fmt.Sprintf(
		`create schema %v`,
		schemaNameInSnowflake,
	)
	_, err = transfer.Target.Db.Exec(
		createSchemaQuery,
	)
	if err != nil {
		return fmt.Errorf("error running create schema query, query was %v. error was: %v", createSchemaQuery, err)
	}

	snowflakeConfig := gosnowflake.Config{
		Account:       transfer.Target.AccountId,
		User:          transfer.Target.Username,
		Database:      transfer.Target.DbName,
		Warehouse:     transfer.Target.Warehouse,
		Role:          transfer.Target.Role,
		Authenticator: gosnowflake.AuthTypeJwt,
		PrivateKey:    &transfer.Target.PrivateKey,
		Schema:        schemaNameInSnowflake,
	}

	targetDsn, err := gosnowflake.DSN(&snowflakeConfig)
	if err != nil {
		return fmt.Errorf("error creating snowflake dsn: %v", err)
	}

	targetDb, err := sql.Open("snowflake", targetDsn)
	if err != nil {
		return fmt.Errorf("error opening snowflake connection: %v", err)
	}

	// ping targetDb
	err = targetDb.Ping()
	if err != nil {
		return fmt.Errorf("error pinging snowflake connection: %v", err)
	}

	// create sqlpipe_csv file format in targetDb
	createFileFormatQuery := `CREATE OR REPLACE FILE FORMAT SQLPIPE_CSV ESCAPE_UNENCLOSED_FIELD = 'NONE' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' COMPRESSION = NONE;`
	_, err = targetDb.Exec(
		createFileFormatQuery,
	)
	if err != nil {
		return fmt.Errorf("error running create file format query, query was %v. error was: %v", createFileFormatQuery, err)
	}

	g := new(errgroup.Group)
	g.SetLimit(10)
	for queryIndexOuter, tableOuter := range transfer.Queries {
		go func(queryIndex int, table data.Query) {
			g.Go(func() error {
				transferRows, err := transfer.Source.Db.Query(table.SourceQuery)
				if err != nil {
					return fmt.Errorf("error running extraction query: %v", err)
				}

				columnInfo := data.ColumnInfo{
					ColumnNames:         []string{},
					ColumnDbTypes:       []string{},
					ColumnScanTypes:     []reflect.Type{},
					ColumnNamesAndTypes: []string{},
					ColumnPrecisions:    []int64{},
					ColumnScales:        []int64{},
					ColumnLengths:       []int64{},
				}

				colTypesFromDriver, err := transferRows.ColumnTypes()
				if err != nil {
					return fmt.Errorf("error getting column types: %v", err)
				}

				for _, colType := range colTypesFromDriver {
					columnInfo.ColumnNames = append(columnInfo.ColumnNames, colType.Name())
					columnInfo.ColumnDbTypes = append(columnInfo.ColumnDbTypes, colType.DatabaseTypeName())
					columnInfo.ColumnScanTypes = append(columnInfo.ColumnScanTypes, colType.ScanType())

					colLen, _ := colType.Length()
					columnInfo.ColumnLengths = append(columnInfo.ColumnLengths, colLen)

					precision, scale, _ := colType.DecimalSize()
					columnInfo.ColumnPrecisions = append(columnInfo.ColumnPrecisions, precision)
					columnInfo.ColumnScales = append(columnInfo.ColumnScales, scale)
				}

				columnInfo.NumCols = len(columnInfo.ColumnNames)

				columnInfo, err = data.GetCreateTableTypes(columnInfo)
				if err != nil {
					return fmt.Errorf("error getting create table types: %v", err)
				}

				sourceSchemaNameHasNonAlnum := data.HasNonAlnum(table.Schema)
				sourceTableNameHasNonAlnum := data.HasNonAlnum(table.Table)

				eitherHasNonAlnum := false

				if sourceSchemaNameHasNonAlnum || sourceTableNameHasNonAlnum {
					eitherHasNonAlnum = true
				}

				tableNameInSnowflake := data.QuoteIfTrue(
					fmt.Sprintf(
						`%v_%v`,
						strings.ToUpper(table.Schema),
						strings.ToUpper(table.Table),
					),
					eitherHasNonAlnum,
				)

				createTablequery := fmt.Sprintf(
					`create table %v.%v (`,
					schemaNameInSnowflake,
					tableNameInSnowflake,
				)

				for _, colNameAndType := range columnInfo.ColumnNamesAndTypes {
					createTablequery = createTablequery + fmt.Sprintf("%v, ", colNameAndType)
				}

				createTablequery = strings.TrimSuffix(createTablequery, ", ")
				createTablequery = createTablequery + ");"
				transfer.Queries[queryIndex].TargetCreateTableQuery = createTablequery

				_, err = targetDb.Exec(
					createTablequery,
				)
				if err != nil {
					return fmt.Errorf("error running create table query, query was %v. error was: %v", createTablequery, err)
				}

				numCols := columnInfo.NumCols

				var stringBuilder strings.Builder
				csvWriter := csv.NewWriter(&stringBuilder)

				colDbTypes := columnInfo.ColumnDbTypes
				vals := make([]interface{}, numCols)
				valPtrs := make([]interface{}, numCols)
				dataInRam := false

				for i := 0; i < numCols; i++ {
					valPtrs[i] = &vals[i]
				}

				rowVals := make([]string, numCols)
				for i := 1; transferRows.Next(); i++ {
					transferRows.Scan(valPtrs...)
					for j := 0; j < numCols; j++ {
						formatter, ok := data.Formatters[colDbTypes[j]]
						if !ok {
							return fmt.Errorf("no formatter for db type %v", colDbTypes[j])
						}
						rowVals[j], err = formatter(vals[j])
						if err != nil {
							return fmt.Errorf("error formatting values for csv file: %v", err)
						}
					}
					err = csvWriter.Write(rowVals)
					if err != nil {
						return fmt.Errorf("error writing values to csv file: %v", err)
					}

					dataInRam = true

					if data.TurboInsertChecker(stringBuilder.Len()) {
						csvWriter.Flush()
						reader, err := data.GetGzipReader(stringBuilder.String())
						if err != nil {
							return fmt.Errorf("error getting gzip reader: %v", err)
						}
						err = data.UploadAndTransfer(reader, app.uploader, table.Table, transfer.Id, transfer.AwsConfig.S3Dir, transfer.AwsConfig.S3Bucket)
						if err != nil {
							return fmt.Errorf("error running upload and transfer: %v", err)
						}
						dataInRam = false
						stringBuilder.Reset()
					}
				}

				if dataInRam {
					csvWriter.Flush()
					reader, err := data.GetGzipReader(stringBuilder.String())
					if err != nil {
						return fmt.Errorf("error getting gzip reader: %v", err)
					}
					err = data.UploadAndTransfer(reader, app.uploader, table.Table, transfer.Id, transfer.AwsConfig.S3Dir, transfer.AwsConfig.S3Bucket)
					if err != nil {
						return fmt.Errorf("error running upload and transfer: %v", err)
					}
				}

				loadingQuery := fmt.Sprintf(
					`copy into %v.%v from s3://%v/%v STORAGE_INTEGRATION = "%v" file_format = (format_name = SQLPIPE_CSV)`,
					schemaNameInSnowflake,
					tableNameInSnowflake,
					transfer.AwsConfig.S3Bucket,
					fmt.Sprintf("%v/%v/%v/", transfer.AwsConfig.S3Dir, transfer.Id, table.Table),
					transfer.Target.StorageIntegration,
					// transfer.Target.FileFormatName,
				)

				_, err = targetDb.Exec(loadingQuery)
				if err != nil {
					return fmt.Errorf("error running copy command, query was %v, error was %v", loadingQuery, err)
				}

				return nil
			})
		}(queryIndexOuter, tableOuter)
	}

	errGroupError := g.Wait()
	if err != nil {
		return fmt.Errorf("error running transfer queries: %v", errGroupError)
	}

	permissionsQuery := fmt.Sprintf(
		"CALL %v.sqlpipe.SP_GRANT_RAW_PROFILE_SCHEMA_ACCESS ('%v', '%v', '');",
		transfer.Target.DbName,
		transfer.Target.DbName,
		schemaNameInSnowflake,
	)

	_, err = targetDb.Exec(permissionsQuery)
	if err != nil {
		return fmt.Errorf("error running permissions query, query was %v, error was %v", permissionsQuery, err)
	}

	return nil
}
