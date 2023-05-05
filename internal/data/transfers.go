package data

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/snowflakedb/gosnowflake"
	"github.com/sqlpipe/mssqltosnowflake/pkg"
	"golang.org/x/sync/errgroup"
)

func hasNonAlnum(word string) bool {
	for _, charCode := range word {
		char := fmt.Sprintf("%c", charCode)
		if !regexp.MustCompile(`^[a-zA-Z0-9]*$`).MatchString(char) {
			return true
		}
	}
	return false
}

func quoteIfTrue(word string, hasNonAlnum bool) string {
	if hasNonAlnum {
		return fmt.Sprintf(`"%v"`, word)
	}
	return word
}

var snowflakeReservedKeywords = map[string]bool{"ACCOUNT": true, "ALL": true, "ALTER": true, "AND": true, "ANY": true, "AS": true, "BETWEEN": true, "BY": true, "CASE": true, "CAST": true, "CHECK": true, "COLUMN": true, "CONNECT": true, "CONNECTION": true, "CONSTRAINT": true, "CREATE": true, "CROSS": true, "CURRENT": true, "CURRENT_DATE": true, "CURRENT_TIME": true, "CURRENT_TIMESTAMP": true, "CURRENT_USER": true, "DATABASE": true, "DELETE": true, "DISTINCT": true, "DROP": true, "ELSE": true, "EXISTS": true, "FALSE": true, "FOLLOWING": true, "FOR": true, "FROM": true, "FULL": true, "GRANT": true, "GROUP": true, "GSCLUSTER": true, "HAVING": true, "ILIKE": true, "IN": true, "INCREMENT": true, "INNER": true, "INSERT": true, "INTERSECT": true, "INTO": true, "IS": true, "ISSUE": true, "JOIN": true, "LATERAL": true, "LEFT": true, "LIKE": true, "LOCALTIME": true, "LOCALTIMESTAMP": true, "MINUS": true, "NATURAL": true, "NOT": true, "NULL": true, "OF": true, "ON": true, "OR": true, "ORDER": true, "ORGANIZATION": true, "QUALIFY": true, "REGEXP": true, "REVOKE": true, "RIGHT": true, "RLIKE": true, "ROW": true, "ROWS": true, "SAMPLE": true, "SCHEMA": true, "SELECT": true, "SET": true, "SOME": true, "START": true, "TABLE": true, "TABLESAMPLE": true, "THEN": true, "TO": true, "TRIGGER": true, "TRUE": true, "TRY_CAST": true, "UNION": true, "UNIQUE": true, "UPDATE": true, "USING": true, "VALUES": true, "VIEW": true, "WHEN": true, "WHENEVER": true, "WHERE": true, "WITH": true}

type Query struct {
	Schema                 string `json:"source_schema"`
	Table                  string `json:"source_table"`
	SourceQuery            string `json:"source_query"`
	S3Path                 string `json:"s3_path"`
	TargetCreateTableQuery string `json:"target_create_table_query"`
	TargetQuery            string `json:"target_query"`
}

type ColumnInfo struct {
	ColumnNames         []string
	ColumnDbTypes       []string
	ColumnScanTypes     []reflect.Type
	ColumnNamesAndTypes []string
	ColumnPrecisions    []int64
	ColumnScales        []int64
	ColumnLengths       []int64
	NumCols             int
}

type Transfer struct {
	Id        string    `json:"transfer_id"`
	CreatedAt time.Time `json:"transfer_created_at"`
	Source    Source    `json:"-"`
	Target    Target    `json:"-"`
	AwsConfig AwsConfig `json:"-"`
	Queries   []Query   `json:"transfer_queries"`
	Status    string    `json:"transfer_status"`
	Error     string    `json:"transfer_error"`
}

func (transfer Transfer) Run() error {
	schemaRows, err := transfer.Source.Db.Query(
		"SELECT S.name as schema_name, T.name as table_name FROM sys.tables AS T INNER JOIN sys.schemas AS S ON S.schema_id = T.schema_id LEFT JOIN sys.extended_properties AS EP ON EP.major_id = T.[object_id] WHERE T.is_ms_shipped = 0 AND (EP.class_desc IS NULL OR (EP.class_desc <>'OBJECT_OR_COLUMN' AND EP.[name] <> 'microsoft_database_tools_support'))",
	)
	if err != nil {
		return fmt.Errorf("error running query getting all db objects: %v", err)
	}
	defer schemaRows.Close()

	var sourceSchema string
	var sourceTable string
	queries := []Query{}
	for schemaRows.Next() {
		err := schemaRows.Scan(&sourceSchema, &sourceTable)
		if err != nil {
			return fmt.Errorf("error scanning schema and table into query object: %v", err)
		}

		sourceQuery := fmt.Sprintf("select * from [%v].[%v]", sourceSchema, sourceTable)

		query := Query{
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
	sourceDbNameHasNonAlnum := hasNonAlnum(transfer.Source.DbName)
	// [Division]_MSSQL_[Source]_[Server]
	schemaNameInSnowflake := quoteIfTrue(
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
		go func(queryIndex int, table Query) {
			g.Go(func() error {
				transferRows, err := transfer.Source.Db.Query(table.SourceQuery)
				if err != nil {
					return fmt.Errorf("error running extraction query: %v", err)
				}

				columnInfo := ColumnInfo{
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

				columnInfo, err = getCreateTableTypes(columnInfo)
				if err != nil {
					return fmt.Errorf("error getting create table types: %v", err)
				}

				sourceSchemaNameHasNonAlnum := hasNonAlnum(table.Schema)
				sourceTableNameHasNonAlnum := hasNonAlnum(table.Table)

				eitherHasNonAlnum := false

				if sourceSchemaNameHasNonAlnum || sourceTableNameHasNonAlnum {
					eitherHasNonAlnum = true
				}

				tableNameInSnowflake := quoteIfTrue(
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
						formatter, ok := formatters[colDbTypes[j]]
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

					if turboInsertChecker(stringBuilder.Len()) {
						csvWriter.Flush()
						reader, err := getGzipReader(stringBuilder.String())
						if err != nil {
							return fmt.Errorf("error getting gzip reader: %v", err)
						}
						err = uploadAndTransfer(reader, &transfer.AwsConfig.Uploader, table.Table, transfer.Id, transfer.AwsConfig.S3Dir, transfer.AwsConfig.S3Bucket)
						if err != nil {
							return fmt.Errorf("error running upload and transfer: %v", err)
						}
						dataInRam = false
						stringBuilder.Reset()
					}
				}

				if dataInRam {
					csvWriter.Flush()
					reader, err := getGzipReader(stringBuilder.String())
					if err != nil {
						return fmt.Errorf("error getting gzip reader: %v", err)
					}
					err = uploadAndTransfer(reader, &transfer.AwsConfig.Uploader, table.Table, transfer.Id, transfer.AwsConfig.S3Dir, transfer.AwsConfig.S3Bucket)
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

func getCreateTableTypes(columnInfo ColumnInfo) (ColumnInfo, error) {

	for colNum := range columnInfo.ColumnDbTypes {

		colName := columnInfo.ColumnNames[colNum]
		dbType := columnInfo.ColumnDbTypes[colNum]

		createType := ""

		switch dbType {
		case "BIGINT":
			createType = "BIGINT"
		case "BIT":
			createType = "BOOLEAN"
		case "INT":
			createType = "INT"
		case "MONEY":
			createType = "TEXT"
		case "SMALLINT":
			createType = "SMALLINT"
		case "SMALLMONEY":
			createType = "TEXT"
		case "TINYINT":
			createType = "TINYINT"
		case "FLOAT":
			createType = "FLOAT"
		case "REAL":
			createType = "FLOAT"
		case "DATE":
			createType = "DATE"
		case "DATETIME2":
			createType = "TIMESTAMP"
		case "DATETIME":
			createType = "TIMESTAMP"
		case "DATETIMEOFFSET":
			createType = "TIMESTAMP"
		case "SMALLDATETIME":
			createType = "TIMESTAMP"
		case "TIME":
			createType = "TIME"
		case "TEXT":
			createType = "TEXT"
		case "NTEXT":
			createType = "TEXT"
		case "BINARY":
			createType = "BINARY"
		case "VARBINARY":
			createType = "BINARY"
		case "UNIQUEIDENTIFIER":
			createType = "TEXT"
		case "XML":
			createType = "TEXT"
		case "IMAGE":
			createType = "BINARY"
		case "DECIMAL":
			createType = "FLOAT"
		case "CHAR":
			createType = "VARCHAR"
		case "VARCHAR":
			createType = "VARCHAR"
		case "NCHAR":
			createType = "VARCHAR"
		case "NVARCHAR":
			createType = "VARCHAR"
		case "SQL_VARIANT":
			createType = "TEXT"
		case "GEOMETRY":
			createType = "BINARY"
		default:
			return columnInfo, fmt.Errorf("unknown type while getting create table types: %v", dbType)
		}

		colName = strings.ToUpper(colName)

		// colHasNonAlnum := hasNonAlnum(colName)

		// colName = quoteIfTrue(colName, colHasNonAlnum)

		// check for snowflake reserved keywords or numbers being the first character or non alphanumeric chars
		if snowflakeReservedKeywords[colName] || !unicode.IsLetter(rune(colName[0])) || hasNonAlnum(colName) {
			colName = fmt.Sprintf(`"%v"`, colName)
		}

		colNameAndType := fmt.Sprintf(`%v %v`, colName, strings.ToUpper(createType))
		columnInfo.ColumnNamesAndTypes = append(columnInfo.ColumnNamesAndTypes, colNameAndType)
	}

	return columnInfo, nil
}

func turboInsertChecker(currentLen int) bool {
	if currentLen == 0 {
		return false
	} else if currentLen%100000 == 0 {
		return true
	} else {
		return false
	}
}

func getGzipReader(contents string) (zr io.Reader, err error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)

	_, err = zw.Write([]byte(contents))
	if err != nil {
		return
	}

	if err = zw.Close(); err != nil {
		return
	}

	return gzip.NewReader(&buf)
}

func uploadAndTransfer(
	reader io.Reader,
	uploader *manager.Uploader,
	tableName string,
	transferId string,
	s3Dir string,
	s3Bucket string,
) error {

	randomChars, err := pkg.RandomCharacters(32)
	if err != nil {
		return err
	}

	s3Path := fmt.Sprintf("%v/%v/%v/%v", s3Dir, transferId, tableName, randomChars)

	_, err = uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: &s3Bucket,
		Key:    aws.String(s3Path),
		Body:   reader,
	})

	return err
}
