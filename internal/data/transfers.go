package data

import (
	"bytes"
	"compress/gzip"
	"context"
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
	"github.com/sqlpipe/mssqltosnowflake/pkg"
)

func HasNonAlnumOrSpace(word string) bool {
	regex := regexp.MustCompile(`^[a-zA-Z0-9]+$`)
	for _, charCode := range word {
		char := fmt.Sprintf("%c", charCode)
		if !regex.MatchString(char) {
			return true
		}
	}
	return false
}

func QuoteIfTrue(word string, hasNonAlnum bool) string {
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
	Id          string    `json:"transfer_id"`
	CreatedAt   time.Time `json:"transfer_created_at"`
	Concurrency int       `json:"concurrency"`
	Source      *Source   `json:"-"`
	Target      *Target   `json:"-"`
	AwsConfig   AwsConfig `json:"-"`
	Queries     []Query   `json:"transfer_queries"`
	Status      string    `json:"transfer_status"`
	Error       string    `json:"transfer_error"`
}

func GetCreateTableTypes(columnInfo ColumnInfo) (ColumnInfo, error) {

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
		if snowflakeReservedKeywords[colName] || !unicode.IsLetter(rune(colName[0])) || HasNonAlnumOrSpace(colName) {
			colName = fmt.Sprintf(`"%v"`, colName)
		}

		colNameAndType := fmt.Sprintf(`%v %v`, colName, strings.ToUpper(createType))
		columnInfo.ColumnNamesAndTypes = append(columnInfo.ColumnNamesAndTypes, colNameAndType)
	}

	return columnInfo, nil
}

func TurboInsertChecker(currentLen int) bool {
	if currentLen == 0 {
		return false
	} else if currentLen > 10000000 {
		return true
	} else {
		return false
	}
}

func GetGzipReader(contents string) (zr io.Reader, err error) {
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

func UploadAndTransfer(
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
