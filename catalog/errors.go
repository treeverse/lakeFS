package catalog

import (
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/db"
)

var (
	ErrIndexMalformed             = errors.New("index error")
	ErrMultipartPathMismatch      = errors.New("invalid path for multipart upload")
	ErrMultipartInvalidPartNumber = errors.New("invalid part number for multipart upload")
	ErrMultipartInvalidPartETag   = errors.New("invalid ETag for multipart upload")
	ErrRepoExists                 = errors.New("repository already exists")
	ErrBranchNotFound             = fmt.Errorf("branch %w", db.ErrNotFound)
	ErrBranchAlreadyExists        = errors.New("branch already exists")
	ErrNoMergeBase                = errors.New("no common merge base found")
	ErrDestinationNotCommitted    = errors.New("destination branch has uncommitted changes")
	ErrMergeConflict              = errors.New("merge conflict")
	ErrMergeUpdateFailed          = errors.New("failed updating merged destination")
	ErrRepoNotFound               = fmt.Errorf("repository %w", db.ErrNotFound)
	ErrMultipartUploadNotFound    = fmt.Errorf("multipart upload %w", db.ErrNotFound)
	ErrOperationNotPermitted      = errors.New("operation not permitted")
	ErrBranchHasDependentBranches = errors.New("branch has dependent branches")
	ErrInvalidLockValue           = errors.New("invalid lock value")
	ErrCommitNotFound             = fmt.Errorf("commit %w", db.ErrNotFound)
	ErrEntryNotFound              = fmt.Errorf("entry %w", db.ErrNotFound)
	ErrNothingToCommit            = errors.New("nothing to commit")
	ErrEntryUpdateFailed          = errors.New("entry update failed")
)
