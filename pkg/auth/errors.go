package auth

import (
	"errors"

	"github.com/treeverse/lakefs/pkg/db"
)

var (
	ErrNotFound                = db.ErrNotFound
	ErrAlreadyExists           = db.ErrAlreadyExists
	ErrNonUnique               = errors.New("more than one user found")
	ErrInvalidArn              = errors.New("invalid ARN")
	ErrInsufficientPermissions = errors.New("insufficient permissions")
	ErrNoField                 = errors.New("no field tagged in struct")
	ErrInvalidAccessKeyID      = errors.New("invalid access key ID")
	ErrInvalidSecretAccessKey  = errors.New("invalid secret access key")
)
