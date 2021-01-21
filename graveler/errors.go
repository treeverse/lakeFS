package graveler

import (
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/db"
)

// Graveler errors
var (
	ErrNotFound                = wrapError(db.ErrNotFound, "not found")
	ErrNotUnique               = errors.New("not unique")
	ErrInvalidValue            = errors.New("invalid value")
	ErrInvalidMergeBase        = fmt.Errorf("only 2 commits allowed in FindMergeBase: %w", ErrInvalidValue)
	ErrInvalidStorageNamespace = fmt.Errorf("storage namespace: %w", ErrInvalidValue)
	ErrInvalidRepositoryID     = fmt.Errorf("repository id: %w", ErrInvalidValue)
	ErrInvalidBranchID         = fmt.Errorf("branch id: %w", ErrInvalidValue)
	ErrInvalidRef              = fmt.Errorf("ref: %w", ErrInvalidValue)
	ErrInvalidCommitID         = fmt.Errorf("commit id: %w", ErrInvalidValue)
	ErrCommitNotFound          = fmt.Errorf("commit %w", ErrNotFound)
	ErrCreateBranchNoCommit    = fmt.Errorf("can't create a branch without commit")
	ErrRepositoryNotFound      = fmt.Errorf("repository %w", ErrNotFound)
	ErrBranchNotFound          = fmt.Errorf("branch %w", ErrNotFound)
	ErrTagNotFound             = fmt.Errorf("tag %w", ErrNotFound)
	ErrRefAmbiguous            = fmt.Errorf("reference is ambiguous: %w", ErrNotFound)
	ErrConflictFound           = errors.New("conflict found")
	ErrBranchExists            = errors.New("branch already exists")
	ErrTagAlreadyExists        = errors.New("tag already exists")
	ErrDirtyBranch             = errors.New("can't apply meta-range on dirty branch")
	ErrMetaRangeNotFound       = errors.New("metarange not found")
	ErrLockNotAcquired         = errors.New("lock not acquired")
)

// wrappedError is an error for wrapping another error while ignoring its message.
type wrappedError struct {
	err error
	msg string
}

func (w *wrappedError) Error() string {
	return w.msg
}

func (w *wrappedError) Unwrap() error {
	return w.err
}

// wrapError returns an error wrapping err with message msg, ignoring the error message from
// err.
func wrapError(err error, msg string) error {
	return &wrappedError{err: err, msg: msg}
}
