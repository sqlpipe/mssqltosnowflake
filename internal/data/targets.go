package data

import (
	"crypto/rsa"
	"database/sql"

	"github.com/sqlpipe/mssqltosnowflake/internal/validator"
)

type Target struct {
	AccountId          string         `json:"target_account_id"`
	Username           string         `json:"target_username"`
	PrivateKeyLocation string         `json:"target_private_key_location"`
	PrivateKey         rsa.PrivateKey `json:"target_private_key"`
	Role               string         `json:"target_role"`
	Warehouse          string         `json:"target_warehouse"`
	AwsRegion          string         `json:"target_aws_region"`
	DbName             string         `json:"target_db_name"`
	Db                 *sql.DB        `json:"target_db"`
	StorageIntegration string         `json:"target_storage_integration"`
	DivisionCode       string         `json:"target_division_code"`
	RootName           string         `json:"target_root_name"`
	// ServerName         string         `json:"server_name"`
	// FileFormatName     string         `json:"file_format_name"`
}

func ValidateTarget(v *validator.Validator, target Target) {
	v.Check(target.AccountId != "", "target_account_id", "must be provided")
	v.Check(target.PrivateKeyLocation != "", "target_private_key_location", "must be provided")
	v.Check(target.Role != "", "target_role", "must be provided")
	v.Check(target.Warehouse != "", "target_warehouse", "must be provided")
	v.Check(target.AwsRegion != "", "target_aws_region", "must be provided")
	v.Check(target.Username != "", "target_username", "must be provided")
	v.Check(target.DbName != "", "target_db_name", "must be provided")
	v.Check(target.StorageIntegration != "", "target_storage_integration", "must be provided")
	v.Check(target.DivisionCode != "", "target_division_code", "must be provided")
	v.Check(target.RootName != "", "target_root_name", "must be provided")
	// v.Check(target.ServerName != "", "server_name", "must be provided")
	// v.Check(target.FileFormatName != "", "target_file_format_name", "must be provided")
}
