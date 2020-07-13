package db

import (
	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
)

func IsSerializationError(err error) bool {
	return isPGCode(err, pgerrcode.SerializationFailure)
}

func IsUniqueViolation(err error) bool {
	return isPGCode(err, pgerrcode.UniqueViolation)
}

func isPGCode(err error, code string) bool {
	if pgErr, ok := err.(*pgconn.PgError); ok {
		return pgErr.Code == code
	}
	return false
}
