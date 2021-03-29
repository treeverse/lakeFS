package catalog

import (
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/db"
)

var (
	ErrNotFound                 = db.ErrNotFound
	ErrInvalid                  = errors.New("validation error")
	ErrInvalidMetadataSrcFormat = errors.New("invalid metadata src format")
	ErrExpired                  = errors.New("expired from storage")
	ErrFeatureNotSupported      = errors.New("feature not supported")
	ErrBranchNotFound           = fmt.Errorf("branch %w", ErrNotFound)
	ErrRepositoryNotFound       = fmt.Errorf("repository %w", ErrNotFound)
	ErrInvalidValue             = fmt.Errorf("invalid value: %w", ErrInvalid)
	ErrNoDifferenceWasFound     = errors.New("no difference was found")
	ErrConflictFound            = errors.New("conflict found")
)
