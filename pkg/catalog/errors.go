package catalog

import (
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/db"
)

var (
	ErrInvalid                  = errors.New("validation error")
	ErrInvalidType              = fmt.Errorf("invalid type: %w", ErrInvalid)
	ErrRequiredValue            = fmt.Errorf("required value: %w", ErrInvalid)
	ErrPathRequiredValue        = fmt.Errorf("missing path: %w", ErrRequiredValue)
	ErrInvalidValue             = fmt.Errorf("invalid value: %w", ErrInvalid)
	ErrNotFound                 = db.ErrNotFound
	ErrInvalidMetadataSrcFormat = errors.New("invalid metadata src format")
	ErrExpired                  = errors.New("expired from storage")
	ErrFeatureNotSupported      = errors.New("feature not supported")
	ErrBranchNotFound           = fmt.Errorf("branch %w", ErrNotFound)
	ErrRepositoryNotFound       = fmt.Errorf("repository %w", ErrNotFound)
	ErrNoDifferenceWasFound     = errors.New("no difference was found")
	ErrConflictFound            = errors.New("conflict found")
	ErrInvalidRef               = errors.New("invalid ref")
)
