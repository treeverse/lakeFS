package authentication

import (
	"errors"
)

var (
	ErrInvalidSTS              = errors.New("invalid sts")
	ErrNotImplemented          = errors.New("not implemented")
	ErrAlreadyExists           = errors.New("already exists")
	ErrInsufficientPermissions = errors.New("insufficient permissions")
	ErrUnexpectedStatusCode    = errors.New("unexpected status code")
	ErrInvalidRequest          = errors.New("invalid request")
)
