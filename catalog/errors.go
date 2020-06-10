package catalog

import (
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/db"
)

var (
	ErrOperationNotPermitted      = errors.New("operation not permitted")
	ErrBranchHasDependentBranches = errors.New("branch has dependent branches")
	ErrInvalidLockValue           = errors.New("invalid lock value")
	ErrInvalidState               = errors.New("invalid state")
	ErrNothingToCommit            = errors.New("nothing to commit")
	ErrEntryUpdateFailed          = errors.New("entry update failed")
	ErrBranchNotFound             = fmt.Errorf("branch %w", db.ErrNotFound)
	ErrRepoNotFound               = fmt.Errorf("repository %w", db.ErrNotFound)
	ErrMultipartUploadNotFound    = fmt.Errorf("multipart upload %w", db.ErrNotFound)
	ErrCommitNotFound             = fmt.Errorf("commit %w", db.ErrNotFound)
	ErrEntryNotFound              = fmt.Errorf("entry %w", db.ErrNotFound)
)
