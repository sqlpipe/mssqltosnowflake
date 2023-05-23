package data

import (
	"github.com/sqlpipe/mssqltosnowflake/internal/validator"
)

type AwsConfig struct {
	// Key      string           `json:"-"`
	// Secret   string           `json:"-"`
	// Token    string           `json:"-"`
	S3Bucket string `json:"aws_config_s3_bucket"`
	S3Dir    string `json:"aws_config_s3_dir"`
	Region   string `json:"aws_config_region"`
	// Uploader manager.Uploader `json:"aws_s3_uploader"`
}

func ValidateAwsConfig(v *validator.Validator, awsConfig AwsConfig) {
	// v.Check(awsConfig.Key != "", "aws_config_key", "must be provided")
	// v.Check(awsConfig.Secret != "", "aws_config_secret", "must be provided")
	v.Check(awsConfig.S3Bucket != "", "aws_config_s3_bucket", "must be provided")
	v.Check(awsConfig.S3Dir != "", "aws_config_s3_dir", "must be provided")
	v.Check(awsConfig.S3Dir != "", "aws_config_region", "must be provided")
}
