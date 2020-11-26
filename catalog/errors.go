package catalog

import (
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/db"
)

var (
	ErrInvalidReference            = errors.New("invalid reference")
	ErrInvalidMetadataSrcFormat    = errors.New("invalid metadata src format")
	ErrExpired                     = errors.New("expired from storage")
	ErrByteSliceTypeAssertion      = errors.New("type assertion to []byte failed")
	ErrFeatureNotSupported         = errors.New("feature not supported")
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
	ErrOperationNotPermitted       = errors.New("operation not permitted")
	ErrNothingToCommit             = errors.New("nothing to commit")
	ErrInvalidLockValue            = errors.New("invalid lock value")
	ErrNoDifferenceWasFound        = errors.New("no difference was found")
	ErrConflictFound               = errors.New("conflict found")
	ErrUnsupportedRelation         = errors.New("unsupported relation")
	ErrUnsupportedDelimiter        = errors.New("unsupported delimiter")
	ErrBadTypeConversion           = errors.New("bad type")
	ErrExportFailed                = errors.New("export failed")
)
