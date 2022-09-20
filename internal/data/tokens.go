package data

import (
	"unicode/utf8"

	"github.com/sqlpipe/mssqltosnowflake/internal/validator"
)

func ValidateTokenPlaintext(v *validator.Validator, tokenPlaintext string) {
	v.Check(tokenPlaintext != "", "token", "must be provided")
	v.Check(utf8.RuneCountInString(tokenPlaintext) == 32, "token", "must be 32 characters long")
}
