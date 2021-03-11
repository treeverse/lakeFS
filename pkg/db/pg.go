package db

import (
	"errors"

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
	var pgErr *pgconn.PgError
	if err != nil && errors.As(err, &pgErr) {
		return pgErr.Code == code
	}
	return false
}
