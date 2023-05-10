package auth

import (
	"errors"

	"github.com/treeverse/lakefs/pkg/kv"
)

var (
	ErrNotFound                = kv.ErrNotFound
	ErrAlreadyExists           = errors.New("already exists")
	ErrNonUnique               = errors.New("more than one user found")
	ErrInvalidArn              = errors.New("invalid ARN")
	ErrInsufficientPermissions = errors.New("insufficient permissions")
	ErrInvalidAccessKeyID      = errors.New("invalid access key ID")
	ErrInvalidSecretAccessKey  = errors.New("invalid secret access key")
	ErrUnexpectedStatusCode    = errors.New("unexpected status code")
	ErrUnexpectedSigningMethod = errors.New("unexpected signing method")
	ErrInvalidToken            = errors.New("invalid token")
	ErrInvalidRequest          = errors.New("invalid request")
	ErrUserNotFound            = errors.New("user not found")
	ErrInvalidResponse         = errors.New("invalid response")
)
