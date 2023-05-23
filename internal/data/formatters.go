package data

import (
	"errors"
	"fmt"
	"time"
)

var Formatters = map[string]func(value interface{}) (string, error){
	"BIT":              csvCastToBoolWriteBinaryEquivalent,
	"FLOAT":            csvPrintRaw,
	"DOUBLE":           csvPrintRaw,
	"REAL":             csvPrintRaw,
	"TINYINT":          csvPrintRaw,
	"SMALLINT":         csvPrintRaw,
	"INT":              csvPrintRaw,
	"BIGINT":           csvPrintRaw,
	"DATETIME2":        csvCastToTimeFormatToTimetampString,
	"DATETIME":         csvCastToTimeFormatToTimetampString,
	"DECIMAL":          csvCastToBytesCastToString,
	"MONEY":            csvCastToBytesCastToString,
	"SMALLMONEY":       csvCastToBytesCastToString,
	"DATE":             csvCastToTimeFormatToTimetampString,
	"DATETIMEOFFSET":   csvCastToTimeFormatToTimetampString,
	"SMALLDATETIME":    csvCastToTimeFormatToTimetampString,
	"TIME":             csvCastToTimeFormatToTimeString,
	"CHAR":             csvPrintRaw,
	"VARCHAR":          csvPrintRaw,
	"TEXT":             csvPrintRaw,
	"NCHAR":            csvPrintRaw,
	"NVARCHAR":         csvPrintRaw,
	"NTEXT":            csvPrintRaw,
	"BINARY":           csvCastToBytesCastToHexString,
	"VARBINARY":        csvCastToBytesCastToHexString,
	"UNIQUEIDENTIFIER": csvCastToBytesWriteMssqlUniqueIdentifier,
	"SQL_VARIANT":      csvPrintRaw,
	"XML":              csvPrintRaw,
	"IMAGE":            csvCastToBytesCastToHexString,
	"GEOMETRY":         csvCastToBytesCastToHexString,
}

func csvPrintRaw(value interface{}) (string, error) {
	if value == nil {
		return "", nil
	}
	formattedValue := fmt.Sprintf(`%v`, value)
	if len(formattedValue) > 16777216 {
		formattedValue = formattedValue[:16777216]
	}
	return formattedValue, nil
}

func csvCastToBoolWriteBinaryEquivalent(value interface{}) (string, error) {
	if value == nil {
		return "", nil
	}
	valBool, ok := value.(bool)
	if !ok {
		return ``, errors.New(`castToBool unable to cast value to bool`)
	}

	switch valBool {
	case true:
		return "1", nil
	default:
		return "0", nil
	}
}

func csvCastToBytesCastToHexString(value interface{}) (string, error) {
	if value == nil {
		return "", nil
	}
	valBytes, ok := value.([]byte)
	if !ok {
		return ``, errors.New(`csvCastToBytesCastToString unable to cast value to bytes`)
	}
	formattedValue := fmt.Sprintf(`%x`, string(valBytes))
	if len(formattedValue) > 16777216 {
		formattedValue = formattedValue[:16777216]
	}
	return formattedValue, nil
}

func csvCastToBytesCastToString(value interface{}) (string, error) {
	if value == nil {
		return "", nil
	}
	valBytes, ok := value.([]byte)
	if !ok {
		return ``, errors.New(`csvCastToBytesCastToString unable to cast value to bytes`)
	}
	formattedValue := fmt.Sprintf(`%v`, string(valBytes))
	if len(formattedValue) > 16777216 {
		formattedValue = formattedValue[:16777216]
	}
	return formattedValue, nil
}

func csvCastToBytesWriteMssqlUniqueIdentifier(value interface{}) (string, error) {
	if value == nil {
		return "", nil
	}
	valBytes, ok := value.([]byte)
	if !ok {
		return ``, errors.New(`csvCastToBytesCastToString unable to cast value to bytes`)
	}
	return fmt.Sprintf(
		"%X%X%X%X%X%X%X%X%X%X%X",
		valBytes[3],
		valBytes[2],
		valBytes[1],
		valBytes[0],
		valBytes[5],
		valBytes[4],
		valBytes[7],
		valBytes[6],
		valBytes[8],
		valBytes[9],
		valBytes[10:],
	), nil
}

func csvCastToTimeFormatToTimeString(value interface{}) (string, error) {
	if value == nil {
		return "", nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return ``, errors.New(`castToTimeFormatToTimeString unable to cast value to bytes`)
	}
	return fmt.Sprintf(`%v`, valTime.Format(`15:04:05.999999999`)), nil
}

func csvCastToTimeFormatToTimetampString(value interface{}) (string, error) {
	if value == nil {
		return "", nil
	}
	valTime, ok := value.(time.Time)
	if !ok {
		return ``, errors.New(`castToTimeFormatToTimetampString unable to cast value to bytes`)
	}
	return fmt.Sprintf(`%v`, valTime.Format("2006-01-02 15:04:05.000000")), nil
}
