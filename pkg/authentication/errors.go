package authentication

import (
	"errors"
)

var (
	ErrNotImplemented          = errors.New("not implemented")
	ErrAlreadyExists           = errors.New("already exists")
	ErrInsufficientPermissions = errors.New("insufficient permissions")
	ErrUnexpectedStatusCode    = errors.New("unexpected status code")
	ErrInvalidRequest          = errors.New("invalid request")
	ErrSessionExpired          = errors.New("session expired")
	ErrInvalidTokenFormat      = errors.New("invalid token format")
)
