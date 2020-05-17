package db

import "github.com/jackc/pgconn"

const (
	PGSerializationFailure = "40001"
)

func isSerializationError(err error) bool {
	if pgErr, ok := err.(*pgconn.PgError); ok {
		return pgErr.Code == PGSerializationFailure
	}
	return false
}
