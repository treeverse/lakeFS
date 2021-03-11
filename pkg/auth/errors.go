package auth

import (
	"errors"

	"github.com/treeverse/lakefs/pkg/db"
)

var (
	ErrNotFound                = db.ErrNotFound
	ErrAlreadyExists           = db.ErrAlreadyExists
	ErrInvalidArn              = errors.New("invalid ARN")
	ErrInsufficientPermissions = errors.New("insufficient permissions")
)
