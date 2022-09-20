package data

import (
	"database/sql"

	"github.com/sqlpipe/mssqltosnowflake/internal/validator"
)

type Source struct {
	Host     string `json:"source_host"`
	Port     int    `json:"source_port"`
	Username string `json:"source_username"`
	Password string `json:"password"`
	DbName   string `json:"source_db_name"`
	Db       sql.DB `json:"source_db"`
}

func ValidateSource(v *validator.Validator, source Source) {
	v.Check(source.Host != "", "source_host", "must be provided")
	v.Check(source.Port != 0, "source_port", "must be provided")
	v.Check(source.Username != "", "source_username", "must be provided")
	v.Check(source.Password != "", "source_password", "must be provided")
	v.Check(source.DbName != "", "source_db_name", "must be provided")
}
