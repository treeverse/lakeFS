package errors

import "errors"

var (
	ErrExpectedType = errors.New("expected type")
	ErrSchemaExists = errors.New("schema exists")
)
