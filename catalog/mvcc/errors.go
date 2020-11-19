package mvcc

import (
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/db"
)

var (
	ErrFeatureNotSupported         = errors.New("feature not supported")
	ErrOperationNotPermitted       = errors.New("operation not permitted")
	ErrInvalidLockValue            = errors.New("invalid lock value")
	ErrNothingToCommit             = errors.New("nothing to commit")
	ErrNoDifferenceWasFound        = errors.New("no difference was found")
	ErrConflictFound               = errors.New("conflict found")
	ErrUnsupportedRelation         = errors.New("unsupported relation")
	ErrUnsupportedDelimiter        = errors.New("unsupported delimiter")
	ErrBranchNotFound              = fmt.Errorf("branch %w", db.ErrNotFound)
	ErrCommitNotFound              = fmt.Errorf("commit %w", db.ErrNotFound)
	ErrRepositoryNotFound          = fmt.Errorf("repository %w", db.ErrNotFound)
	ErrMultipartUploadNotFound     = fmt.Errorf("multipart upload %w", db.ErrNotFound)
	ErrEntryNotFound               = fmt.Errorf("entry %w", db.ErrNotFound)
	ErrUnexpected                  = errors.New("unexpected error")
	ErrReadEntryTimeout            = errors.New("read entry timeout")
	ErrInvalidValue                = errors.New("invalid value")
	ErrNonDirectNotSupported       = errors.New("non direct diff not supported")
	ErrSameBranchMergeNotSupported = errors.New("same branch merge not supported")
	ErrLineageCorrupted            = errors.New("lineage corrupted")
)
