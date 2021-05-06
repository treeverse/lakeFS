package graveler

import (
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/db"
)

var (
	// Base error for "user-visible" errors, which should not be wrapped with internal
	// debug info.
	ErrUserVisible = errors.New("")

	// TODO(ariels): Wrap with ErrUserVisible once db is gone.
	ErrNotFound               = wrapError(db.ErrNotFound, "not found")
	ErrNotUnique              = errors.New("not unique")
	ErrPreconditionFailed     = errors.New("precondition failed")
	ErrInvalidValue           = errors.New("invalid value")
	ErrInvalidMergeBase       = fmt.Errorf("only 2 commits allowed in FindMergeBase: %w", ErrInvalidValue)
	ErrNoMergeBase            = errors.New("no merge base")
	ErrInvalidRef             = fmt.Errorf("ref: %w", ErrInvalidValue)
	ErrInvalidCommitID        = fmt.Errorf("commit id: %w", ErrInvalidValue)
	ErrCommitNotFound         = fmt.Errorf("commit %w", ErrNotFound)
	ErrCreateBranchNoCommit   = fmt.Errorf("can't create a branch without commit")
	ErrRepositoryNotFound     = fmt.Errorf("repository %w", ErrNotFound)
	ErrBranchNotFound         = fmt.Errorf("branch %w", ErrNotFound)
	ErrTagNotFound            = fmt.Errorf("tag %w", ErrNotFound)
	ErrRefAmbiguous           = fmt.Errorf("reference is ambiguous: %w", ErrNotFound)
	ErrNoChanges              = wrapError(ErrUserVisible, "no changes")
	ErrConflictFound          = errors.New("conflict found")
	ErrCommitNotHeadBranch    = errors.New("commit is not head of branch")
	ErrBranchExists           = fmt.Errorf("branch already exists: %w", ErrNotUnique)
	ErrTagAlreadyExists       = fmt.Errorf("tag already exists: %w", ErrNotUnique)
	ErrDirtyBranch            = errors.New("can't apply meta-range on dirty branch")
	ErrMetaRangeNotFound      = errors.New("metarange not found")
	ErrLockNotAcquired        = errors.New("lock not acquired")
	ErrRevertMergeNoParent    = errors.New("must specify 1-based parent number for reverting merge commit")
	ErrAddCommitNoParent      = errors.New("added commit must have a parent")
	ErrMultipleParents        = errors.New("cannot have more than a single parent")
	ErrRevertParentOutOfRange = errors.New("given commit does not have the given parent number")
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

// HookAbortError abort by hook error, holds the event type with the run id to trace back the run
type HookAbortError struct {
	EventType EventType
	RunID     string
	Err       error
}

func (e *HookAbortError) Error() string {
	return fmt.Sprintf("%s hook aborted, run id '%s': %s", e.EventType, e.RunID, e.Err)
}

func (e *HookAbortError) Unwrap() error {
	return e.Err
}
