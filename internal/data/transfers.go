package data

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sqlpipe/mssqltosnowflake/pkg"
	"golang.org/x/sync/errgroup"
)

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
	Source    Source    `json:"transfer_source"`
	Target    Target    `json:"transfer_target"`
	AwsConfig AwsConfig `json:"transfer_aws_config"`
	Queries   []Query   `json:"transfer_queries"`
}

func (transfer Transfer) Run(ctx context.Context) error {
	schemaRows, err := transfer.Source.Db.Query(
		"SELECT S.name as schema_name, T.name as table_name FROM sys.tables AS T INNER JOIN sys.schemas AS S ON S.schema_id = T.schema_id LEFT JOIN sys.extended_properties AS EP ON EP.major_id = T.[object_id] WHERE T.is_ms_shipped = 0 AND (EP.class_desc IS NULL OR (EP.class_desc <>'OBJECT_OR_COLUMN' AND EP.[name] <> 'microsoft_database_tools_support'))",
	)
	if err != nil {
		return fmt.Errorf("error running query getting all db objects: %v", err)
	}
	defer schemaRows.Close()

	// awsTokenConfig := ""
	// if transfer.AwsConfig.Token != "" {
	// 	awsTokenConfig = fmt.Sprintf("aws_token='%v'", transfer.AwsConfig.Token)
	// }

	var sourceSchema string
	var sourceTable string
	queries := []Query{}
	for schemaRows.Next() {
		err := schemaRows.Scan(&sourceSchema, &sourceTable)
		if err != nil {
			return fmt.Errorf("error scanning schema and table into query object: %v", err)
		}

		targetTable := fmt.Sprintf("%v_%v", sourceSchema, sourceTable)
		randomCharacters, err := pkg.RandomCharacters(32)
		if err != nil {
			return fmt.Errorf("error generating random characters: %v", err)
		}

		sourceQuery := fmt.Sprintf("select * from %v.%v", sourceSchema, sourceTable)
		s3Path := fmt.Sprintf("%v/%v/%v/%v", transfer.AwsConfig.S3Dir, transfer.Id, targetTable, randomCharacters)

		query := Query{
			Schema:      sourceSchema,
			Table:       sourceTable,
			SourceQuery: sourceQuery,
			S3Path:      s3Path,
		}

		queries = append(queries, query)
	}
	err = schemaRows.Err()
	if err != nil {
		return fmt.Errorf("error iterating over schemaRows: %v", err)
	}

	transfer.Queries = queries

	dropSchemaQuery := fmt.Sprintf(
		"drop schema if exists MSSQL_%v",
		transfer.Source.DbName,
	)
	_, err = transfer.Target.Db.ExecContext(
		ctx,
		dropSchemaQuery,
	)
	if err != nil {
		return fmt.Errorf("error running drop schema query, query was %v. error was: %v", dropSchemaQuery, err)
	}

	createSchemaQuery := fmt.Sprintf(
		"create schema MSSQL_%v",
		transfer.Source.DbName,
	)
	_, err = transfer.Target.Db.ExecContext(
		ctx,
		createSchemaQuery,
	)
	if err != nil {
		return fmt.Errorf("error running create schema query, query was %v. error was: %v", createSchemaQuery, err)
	}

	g, ctx := errgroup.WithContext(ctx)

	g.SetLimit(10)

	for outerLoopI, outerLoopTable := range transfer.Queries {

		i := outerLoopI
		table := outerLoopTable
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

			// table.Schema = strings.ReplaceAll(table.Schema, " ", "_")
			// table.Table = strings.ReplaceAll(table.Schema, " ", "_")
			createTablequery := fmt.Sprintf("create table MSSQL_%v.%v_%v (", transfer.Source.DbName, table.Schema, table.Table)

			for _, colNameAndType := range columnInfo.ColumnNamesAndTypes {
				createTablequery = createTablequery + fmt.Sprintf("%v, ", colNameAndType)
			}

			createTablequery = strings.TrimSuffix(createTablequery, ", ")
			createTablequery = createTablequery + ");"
			transfer.Queries[i].TargetCreateTableQuery = createTablequery

			_, err = transfer.Target.Db.ExecContext(
				ctx,
				createTablequery,
			)
			if err != nil {
				return fmt.Errorf("error running create table query, query was %v. error was: %v", createTablequery, err)
			}

			numCols := columnInfo.NumCols
			// zeroIndexedNumCols := numCols - 1

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
					rowVals[j], err = formatters[colDbTypes[j]](vals[j])
					if err != nil {
						return fmt.Errorf("error formatting values for csv file: %v", err)
					}
				}
				err = csvWriter.Write(rowVals)
				if err != nil {
					return fmt.Errorf("error writing values to csv file: %v", err)
				}

				// while in the middle of insert row, add commas at end of values
				// for j := 0; j < zeroIndexedNumCols; j++ {
				// 	turboWritersMid[columnInfo.ColumnDbTypes[j]](vals[j], &fileBuilder)
				// }

				// end of row doesn't need a comma at the end
				// turboWritersEnd[columnInfo.ColumnDbTypes[zeroIndexedNumCols]](vals[zeroIndexedNumCols], &fileBuilder)
				dataInRam = true

				// each dsConn has its own limits on insert statements (either on total
				// length or number of rows)

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
				"copy into %v.%v from s3://%v/%v credentials=(aws_key_id='%v' aws_secret_key='%v' aws_token='%v') file_format = (format_name = %v)",
				fmt.Sprintf("MSSQL_%v", transfer.Source.DbName),
				fmt.Sprintf("%v_%v", table.Schema, table.Table),
				transfer.AwsConfig.S3Bucket,
				fmt.Sprintf("%v/%v/%v/", transfer.AwsConfig.S3Dir, transfer.Id, table.Table),
				transfer.AwsConfig.Key,
				transfer.AwsConfig.Secret,
				transfer.AwsConfig.Token,
				transfer.Target.FileFormatName,
			)

			_, err = transfer.Target.Db.ExecContext(ctx, loadingQuery)
			if err != nil {
				return fmt.Errorf("error running copy command, query was %v, error was %v", loadingQuery, err)
			}

			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		return err
	}

	return nil
}

func getCreateTableTypes(columnInfo ColumnInfo) (ColumnInfo, error) {

	for colNum := range columnInfo.ColumnDbTypes {

		colName := columnInfo.ColumnNames[colNum]
		scanType := columnInfo.ColumnScanTypes[colNum]
		dbType := columnInfo.ColumnDbTypes[colNum]

		createType := ""

		switch scanType.Name() {
		// Generics
		case "bool":
			createType = "BOOLEAN"
		case "int", "int8", "int16", "int32", "uint8", "uint16":
			createType = "INTEGER"
		case "int64", "uint32", "uint64":
			createType = "BIGINT"
		case "float32", "float64":
			createType = "FLOAT"
		case "Time":
			createType = "TIMESTAMP"
		}

		if createType == "" {
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
				createType = "TIMESTAMP"
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
			default:
				createType = "TEXT"
			}
		}
		colNameAndType := fmt.Sprintf("\"%v\" %v", colName, createType)
		columnInfo.ColumnNamesAndTypes = append(columnInfo.ColumnNamesAndTypes, colNameAndType)
	}

	return columnInfo, nil
}

func turboWriteMidVal(valType string, value interface{}, builder *strings.Builder) {
	turboWritersMid[valType](value, builder)
}

func turboWriteEndVal(valType string, value interface{}, builder *strings.Builder) {
	turboWritersEnd[valType](value, builder)
}

var generalReplacer = strings.NewReplacer(`"`, `""`, "\n", "")
var turboEndStringNilReplacer = strings.NewReplacer(
	`"%!s(<nil>)"`, "",
	`%!s(<nil>)`, "",
	`%!x(<nil>)`, "",
	`%!d(<nil>)`, "",
	`%!t(<nil>)`, "",
	`%!v(<nil>)`, "",
	`%!g(<nil>)`, "",
)

var sqlEndStringNilReplacer = strings.NewReplacer(
	`'%!s(<nil>)'`, "null",
	`%!s(<nil>)`, "null",
	`%!d(<nil>)`, "null",
	`%!t(<nil>)`, "null",
	`'%!v(<nil>)'`, "null",
	`%!g(<nil>)`, "null",
	`'\x%!x(<nil>)'`, "null",
	`x'%!x(<nil>)'`, "null",
	`CONVERT(VARBINARY(8000), '0x%!x(<nil>)', 1)`, "null",
	`hextoraw('%!x(<nil>)')`, "null",
	`to_binary('%!x(<nil>)')`, "null",
	`'%!x(<nil>)'`, "null",
	`'%!b(<nil>)'`, "null",
	"'<nil>'", "null",
	"<nil>", "null",
)

var turboWritersMid = map[string]func(value interface{}, builder *strings.Builder){

	// Generics
	"bool":    writeBoolMidTurbo,
	"float32": writeFloatMidTurbo,
	"float64": writeFloatMidTurbo,
	"int16":   writeIntMidTurbo,
	"int32":   writeIntMidTurbo,
	"int64":   writeIntMidTurbo,
	"Time":    snowflakeWriteTimestampFromTimeMidTurbo,

	// MSSQL

	"BIGINT":           writeIntMidTurbo,
	"BIT":              writeBoolMidTurbo,
	"DECIMAL":          writeStringNoQuotesMidTurbo,
	"INT":              writeIntMidTurbo,
	"MONEY":            writeQuotedStringMidTurbo,
	"SMALLINT":         writeIntMidTurbo,
	"SMALLMONEY":       writeQuotedStringMidTurbo,
	"TINYINT":          writeIntMidTurbo,
	"FLOAT":            writeFloatMidTurbo,
	"REAL":             writeFloatMidTurbo,
	"DATE":             snowflakeWriteTimestampFromTimeMidTurbo,
	"DATETIME2":        snowflakeWriteTimestampFromTimeMidTurbo,
	"DATETIME":         snowflakeWriteTimestampFromTimeMidTurbo,
	"DATETIMEOFFSET":   snowflakeWriteTimestampFromTimeMidTurbo,
	"SMALLDATETIME":    snowflakeWriteTimestampFromTimeMidTurbo,
	"TIME":             snowflakeWriteTimestampFromTimeMidTurbo,
	"CHAR":             writeEscapedQuotedStringMidTurbo,
	"VARCHAR":          writeEscapedQuotedStringMidTurbo,
	"TEXT":             writeEscapedQuotedStringMidTurbo,
	"NCHAR":            writeEscapedQuotedStringMidTurbo,
	"NVARCHAR":         writeEscapedQuotedStringMidTurbo,
	"NTEXT":            writeEscapedQuotedStringMidTurbo,
	"BINARY":           snowflakeWriteBinaryfromBytesMidTurbo,
	"VARBINARY":        snowflakeWriteBinaryfromBytesMidTurbo,
	"UNIQUEIDENTIFIER": writeMSSQLUniqueIdentifierMidTurbo,
	"XML":              writeEscapedQuotedStringMidTurbo,
	"IMAGE":            snowflakeWriteBinaryfromBytesMidTurbo,
}

var turboWritersEnd = map[string]func(value interface{}, builder *strings.Builder){

	// Generics
	"bool":    writeBoolEndTurbo,
	"float32": writeFloatEndTurbo,
	"float64": writeFloatEndTurbo,
	"int16":   writeIntEndTurbo,
	"int32":   writeIntEndTurbo,
	"int64":   writeIntEndTurbo,
	"Time":    snowflakeWriteTimestampFromTimeEndTurbo,

	// MSSQL

	"BIGINT":           writeIntEndTurbo,
	"BIT":              writeBoolEndTurbo,
	"DECIMAL":          writeStringNoQuotesEndTurbo,
	"INT":              writeIntEndTurbo,
	"MONEY":            writeQuotedStringEndTurbo,
	"SMALLINT":         writeIntEndTurbo,
	"SMALLMONEY":       writeQuotedStringEndTurbo,
	"TINYINT":          writeIntEndTurbo,
	"FLOAT":            writeFloatEndTurbo,
	"REAL":             writeFloatEndTurbo,
	"DATE":             snowflakeWriteTimestampFromTimeEndTurbo,
	"DATETIME2":        snowflakeWriteTimestampFromTimeEndTurbo,
	"DATETIME":         snowflakeWriteTimestampFromTimeEndTurbo,
	"DATETIMEOFFSET":   snowflakeWriteTimestampFromTimeEndTurbo,
	"SMALLDATETIME":    snowflakeWriteTimestampFromTimeEndTurbo,
	"TIME":             snowflakeWriteTimestampFromTimeEndTurbo,
	"CHAR":             writeEscapedQuotedStringEndTurbo,
	"VARCHAR":          writeEscapedQuotedStringEndTurbo,
	"TEXT":             writeEscapedQuotedStringEndTurbo,
	"NCHAR":            writeEscapedQuotedStringEndTurbo,
	"NVARCHAR":         writeEscapedQuotedStringEndTurbo,
	"NTEXT":            writeEscapedQuotedStringEndTurbo,
	"BINARY":           snowflakeWriteBinaryfromBytesEndTurbo,
	"VARBINARY":        snowflakeWriteBinaryfromBytesEndTurbo,
	"UNIQUEIDENTIFIER": writeMSSQLUniqueIdentifierEndTurbo,
	"XML":              writeEscapedQuotedStringEndTurbo,
	"IMAGE":            snowflakeWriteBinaryfromBytesEndTurbo,
}

func writeBoolMidTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, "%t,", value)
}
func writeBoolEndTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, "%t\n", value)
}

func writeFloatMidTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, "%g,", value)
}
func writeFloatEndTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, "%g\n", value)
}

func writeIntMidTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, "%d,", value)
}

func writeIntEndTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, "%d\n", value)
}

var snowflakeTimeFormat = "2006-01-02 15:04:05.000000"

func snowflakeWriteTimestampFromTimeMidTurbo(value interface{}, builder *strings.Builder) {
	switch value := value.(type) {
	case time.Time:
		fmt.Fprintf(builder, `%s,`, value.Format(snowflakeTimeFormat))
	default:
		builder.WriteString(",")
	}
}

func snowflakeWriteTimestampFromTimeEndTurbo(value interface{}, builder *strings.Builder) {
	switch value := value.(type) {
	case time.Time:
		fmt.Fprintf(builder, "%s\n", value.Format(snowflakeTimeFormat))
	default:
		builder.WriteString("\n")
	}
}

func writeStringNoQuotesMidTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, `%s,`, value)
}

func writeStringNoQuotesEndTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, "%s\n", value)
}

func writeQuotedStringMidTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, `"%s",`, value)
}

func writeQuotedStringEndTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, "\"%s\"\n", value)
}

func writeEscapedQuotedStringMidTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, `"%s",`, generalReplacer.Replace(fmt.Sprintf("%s", value)))
}

func writeEscapedQuotedStringEndTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, "\"%s\"\n", generalReplacer.Replace(fmt.Sprintf("%s", value)))
}

func snowflakeWriteBinaryfromBytesMidTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, "%x,", value)
}

func snowflakeWriteBinaryfromBytesEndTurbo(value interface{}, builder *strings.Builder) {
	fmt.Fprintf(builder, "%x\n", value)
}

func writeMSSQLUniqueIdentifierMidTurbo(value interface{}, builder *strings.Builder) {
	// This is a really stupid fix but it works
	// https://github.com/denisenkom/go-mssqldb/issues/56
	// I guess the bits get shifted around in the first half of these bytes... whatever
	switch value := value.(type) {
	case []uint8:
		fmt.Fprintf(
			builder,
			"%X%X%X%X%X%X%X%X%X%X%X,",
			value[3],
			value[2],
			value[1],
			value[0],
			value[5],
			value[4],
			value[7],
			value[6],
			value[8],
			value[9],
			value[10:],
		)
	default:
		builder.WriteString(",")
	}
}

func writeMSSQLUniqueIdentifierEndTurbo(value interface{}, builder *strings.Builder) {
	// This is a really stupid fix but it works
	// https://github.com/denisenkom/go-mssqldb/issues/56
	// I guess the bits get shifted around in the first half of these bytes... whatever
	switch value := value.(type) {
	case []uint8:
		fmt.Fprintf(
			builder,
			"%X%X%X%X%X%X%X%X%X%X%X\n",
			value[3],
			value[2],
			value[1],
			value[0],
			value[5],
			value[4],
			value[7],
			value[6],
			value[8],
			value[9],
			value[10:],
		)
	default:
		builder.WriteString("\n")
	}
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
