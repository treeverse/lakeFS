package catalog

import (
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/db"
)

var (
	ErrNotFound                 = db.ErrNotFound
	ErrInvalidMetadataSrcFormat = errors.New("invalid metadata src format")
	ErrExpired                  = errors.New("expired from storage")
	ErrFeatureNotSupported      = errors.New("feature not supported")
	ErrBranchNotFound           = fmt.Errorf("branch %w", ErrNotFound)
	ErrRepositoryNotFound       = fmt.Errorf("repository %w", ErrNotFound)
	ErrInvalidValue             = errors.New("invalid value")
	ErrNoDifferenceWasFound     = errors.New("no difference was found")
	ErrConflictFound            = errors.New("conflict found")
)
