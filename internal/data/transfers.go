package data

import (
	"context"
	"fmt"
	"time"

	"github.com/sqlpipe/mssqltosnowflake/pkg"
)

type Query struct {
	SourceQuery string `json:"source_query"`
	S3Path      string `json:"s3_path"`
	TargetQuery string `json:"target_query"`
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
	rows, err := transfer.Source.Db.Query(
		"SELECT S.name as schema_name, T.name as table_name FROM sys.tables AS T INNER JOIN sys.schemas AS S ON S.schema_id = T.schema_id LEFT JOIN sys.extended_properties AS EP ON EP.major_id = T.[object_id] WHERE T.is_ms_shipped = 0 AND (EP.class_desc IS NULL OR (EP.class_desc <>'OBJECT_OR_COLUMN' AND EP.[name] <> 'microsoft_database_tools_support'))",
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	awsTokenConfig := ""
	if transfer.AwsConfig.Token != "" {
		awsTokenConfig = fmt.Sprintf("aws_token='%v'", transfer.AwsConfig.Token)
	}

	var sourceSchema string
	var sourceTable string
	queries := []Query{}
	for rows.Next() {
		err := rows.Scan(&sourceSchema, &sourceTable)
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
			"copy into %v.%v from s3://%v/%v credentials=(aws_key_id='%v' aws_secret_key='%v' %v) file_format = (format_name = %v)",
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
			SourceQuery: sourceQuery,
			S3Path:      s3Path,
			TargetQuery: targetQuery,
		}

		queries = append(queries, query)
		fmt.Println(query)
	}
	err = rows.Err()
	if err != nil {
		return err
	}

	transfer.Queries = queries

	// _, err = transfer.Target.Db.ExecContext(
	// 	ctx,
	// 	fmt.Sprintf(
	// 		"drop schema if exists %v.MSSQL_%v",
	// 		transfer.Target.DbName,
	// 		transfer.Source.DbName,
	// 	),
	// )

	// _, err = transfer.Target.Db.ExecContext(
	// 	ctx,
	// 	fmt.Sprintf(
	// 		"create schema %v.MSSQL_%v",
	// 		transfer.Target.DbName,
	// 		transfer.Source.DbName,
	// 	),
	// )

	return nil
}
