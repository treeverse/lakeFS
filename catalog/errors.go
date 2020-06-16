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
	ErrNoDifferenceWasFound       = errors.New("no difference was found")
	ErrConflictFound              = errors.New("conflict found")
	ErrUnsupportedRelation        = errors.New("unsupported relation")
	ErrBranchNotFound             = fmt.Errorf("branch %w", db.ErrNotFound)
	ErrRepoNotFound               = fmt.Errorf("repository %w", db.ErrNotFound)
	ErrMultipartUploadNotFound    = fmt.Errorf("multipart upload %w", db.ErrNotFound)
	ErrEntryNotFound              = fmt.Errorf("entry %w", db.ErrNotFound)
)
