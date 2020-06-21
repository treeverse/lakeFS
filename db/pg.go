package db

import "github.com/jackc/pgconn"

const (
	PGSerializationFailure = "40001"
	PGUniqueViolation      = "23505"
)

func IsSerializationError(err error) bool {
	return isPGCode(err, PGSerializationFailure)
}

func IsUniqueViolation(err error) bool {
	return isPGCode(err, PGUniqueViolation)
}

func isPGCode(err error, code string) bool {
	if pgErr, ok := err.(*pgconn.PgError); ok {
		return pgErr.Code == code
	}
	return false
}
