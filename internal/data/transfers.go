package data

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/sqlpipe/mssqltosnowflake/pkg"
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
		return err
	}
	defer schemaRows.Close()

	awsTokenConfig := ""
	if transfer.AwsConfig.Token != "" {
		awsTokenConfig = fmt.Sprintf("aws_token='%v'", transfer.AwsConfig.Token)
	}

	var sourceSchema string
	var sourceTable string
	queries := []Query{}
	for schemaRows.Next() {
		err := schemaRows.Scan(&sourceSchema, &sourceTable)
		if err != nil {
			return err
		}

		targetTable := fmt.Sprintf("%v_%v", sourceSchema, sourceTable)
		randomCharacters, err := pkg.RandomCharacters(32)
		if err != nil {
			return err
		}

		sourceQuery := fmt.Sprintf("select * from %v.%v", sourceSchema, sourceTable)
		s3Path := fmt.Sprintf("%v/%v/%v/%v", transfer.AwsConfig.S3Dir, transfer.Id, targetTable, randomCharacters)
		targetQuery := fmt.Sprintf(
			"copy into MSSQL_%v.%v from s3://%v/%v credentials=(aws_key_id='%v' aws_secret_key='%v' %v) file_format = (format_name = %v)",
			transfer.Source.DbName,
			targetTable,
			transfer.AwsConfig.S3Bucket,
			transfer.AwsConfig.S3Dir,
			transfer.AwsConfig.Key,
			transfer.AwsConfig.Secret,
			awsTokenConfig,
			transfer.Target.FileFormatName,
		)

		query := Query{
			Schema:      sourceSchema,
			Table:       sourceTable,
			SourceQuery: sourceQuery,
			S3Path:      s3Path,
			TargetQuery: targetQuery,
		}

		queries = append(queries, query)
	}
	err = schemaRows.Err()
	if err != nil {
		return err
	}

	transfer.Queries = queries

	_, err = transfer.Target.Db.ExecContext(
		ctx,
		fmt.Sprintf(
			"drop schema if exists MSSQL_%v",
			transfer.Source.DbName,
		),
	)
	if err != nil {
		return err
	}

	_, err = transfer.Target.Db.ExecContext(
		ctx,
		fmt.Sprintf(
			"create schema MSSQL_%v",
			transfer.Source.DbName,
		),
	)
	if err != nil {
		return err
	}

	for i, table := range transfer.Queries {

		transferRows, err := transfer.Source.Db.Query(table.SourceQuery)
		if err != nil {
			return err
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
			return err
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

		columnInfo, err = getCreateTableTypes(columnInfo)
		if err != nil {
			return err
		}

		query := fmt.Sprintf("create table MSSQL_%v.%v_%v (", transfer.Source.DbName, table.Schema, table.Table)

		for _, colNameAndType := range columnInfo.ColumnNamesAndTypes {
			query = query + fmt.Sprintf("%v, ", colNameAndType)
		}

		query = strings.TrimSuffix(query, ", ")
		query = query + ");"
		transfer.Queries[i].TargetCreateTableQuery = query

		_, err = transfer.Target.Db.ExecContext(
			ctx,
			query,
		)
		if err != nil {
			return err
		}

		transferRows.Close()
	}

	// transferJsonBytes, _ := json.Marshal(transfer)
	// fmt.Println(string(transferJsonBytes))

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
				createType = "TIMESTAMP_TZ"
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
				createType = fmt.Sprintf(
					"NUMBER(%d,%d)",
					columnInfo.ColumnPrecisions[colNum],
					columnInfo.ColumnScales[colNum],
				)
			case "CHAR":
				createType = fmt.Sprintf(
					"CHAR(%d)",
					columnInfo.ColumnLengths[colNum],
				)
			case "VARCHAR":
				createType = fmt.Sprintf(
					"VARCHAR(%d)",
					columnInfo.ColumnLengths[colNum],
				)
			case "NCHAR":
				createType = fmt.Sprintf(
					"CHAR(%d)",
					columnInfo.ColumnLengths[colNum],
				)
			case "NVARCHAR":
				createType = fmt.Sprintf(
					"VARCHAR(%d)",
					columnInfo.ColumnLengths[colNum],
				)
			default:
				createType = "TEXT"
			}
		}
		colNameAndType := fmt.Sprintf("\"%v\" %v", colName, createType)
		columnInfo.ColumnNamesAndTypes = append(columnInfo.ColumnNamesAndTypes, colNameAndType)
	}

	return columnInfo, nil
}
