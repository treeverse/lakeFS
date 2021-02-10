package catalog

import (
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/db"
)

var (
	ErrInvalidMetadataSrcFormat = errors.New("invalid metadata src format")
	ErrExpired                  = errors.New("expired from storage")
	ErrFeatureNotSupported      = errors.New("feature not supported")
	ErrBranchNotFound           = fmt.Errorf("branch %w", db.ErrNotFound)
	ErrRepositoryNotFound       = fmt.Errorf("repository %w", db.ErrNotFound)
	ErrInvalidValue             = errors.New("invalid value")
	ErrNoDifferenceWasFound     = errors.New("no difference was found")
	ErrConflictFound            = errors.New("conflict found")
	ErrUnsupportedRelation      = errors.New("unsupported relation")
)
