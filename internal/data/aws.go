package data

import (
	"github.com/sqlpipe/mssqltosnowflake/internal/validator"
)

type Aws struct {
	Key      string `json:"-"`
	Secret   string `json:"-"`
	Token    string `json:"-"`
	S3Bucket string `json:"aws_s3_bucket"`
	S3Dir    string `json:"aws_s3_dir"`
}

func ValidateAws(v *validator.Validator, aws *Aws) {
	v.Check(aws.Key != "", "aws_key", "must be provided")
	v.Check(aws.Secret != "", "aws_secret", "must be provided")
	v.Check(aws.S3Bucket != "", "aws_s3_bucket", "must be provided")
	v.Check(aws.S3Dir != "", "aws_s3_dir", "must be provided")
}
