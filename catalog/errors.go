package catalog

import (
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/db"
)

var (
	ErrFeatureNotSupported      = errors.New("feature not supported")
	ErrOperationNotPermitted    = errors.New("operation not permitted")
	ErrInvalidLockValue         = errors.New("invalid lock value")
	ErrNothingToCommit          = errors.New("nothing to commit")
	ErrNoDifferenceWasFound     = errors.New("no difference was found")
	ErrConflictFound            = errors.New("conflict found")
	ErrUnsupportedRelation      = errors.New("unsupported relation")
	ErrUnsupportedDelimiter     = errors.New("unsupported delimiter")
	ErrInvalidReference         = errors.New("invalid reference")
	ErrBranchNotFound           = fmt.Errorf("branch %w", db.ErrNotFound)
	ErrCommitNotFound           = fmt.Errorf("commit %w", db.ErrNotFound)
	ErrRepositoryNotFound       = fmt.Errorf("repository %w", db.ErrNotFound)
	ErrMultipartUploadNotFound  = fmt.Errorf("multipart upload %w", db.ErrNotFound)
	ErrEntryNotFound            = fmt.Errorf("entry %w", db.ErrNotFound)
	ErrByteSliceTypeAssertion   = errors.New("type assertion to []byte failed")
	ErrInvalidMetadataSrcFormat = errors.New("invalid metadata src format")
	ErrUnexpected               = errors.New("unexpected error")
	ErrTimeout                  = errors.New("read entry timeout")
)
