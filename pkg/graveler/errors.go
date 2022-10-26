package graveler

import (
	"errors"
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/treeverse/lakefs/pkg/kv"
)

var (
	// ErrUserVisible is base error for "user-visible" errors, which should not be wrapped with internal debug info.
	ErrUserVisible = errors.New("")

	// TODO(ariels): Wrap with ErrUserVisible once db is gone.
	ErrNotFound                     = wrapError(kv.ErrNotFound, "not found")
	ErrNotUnique                    = wrapError(ErrUserVisible, "not unique")
	ErrPreconditionFailed           = errors.New("precondition failed")
	ErrWriteToProtectedBranch       = wrapError(ErrUserVisible, "cannot write to protected branch")
	ErrReadingFromStore             = errors.New("cannot read from store")
	ErrCommitToProtectedBranch      = wrapError(ErrUserVisible, "cannot commit to protected branch")
	ErrInvalidValue                 = fmt.Errorf("invalid value: %w", ErrInvalid)
	ErrInvalidMergeBase             = fmt.Errorf("only 2 commits allowed in FindMergeBase: %w", ErrInvalidValue)
	ErrNoCommitGeneration           = errors.New("no commit generation")
	ErrNoMergeBase                  = errors.New("no merge base")
	ErrInvalidRef                   = fmt.Errorf("ref: %w", ErrInvalidValue)
	ErrInvalidCommitID              = fmt.Errorf("commit id: %w", ErrInvalidValue)
	ErrInvalidBranchID              = fmt.Errorf("branch id: %w", ErrInvalidValue)
	ErrInvalidTagID                 = fmt.Errorf("tag id: %w", ErrInvalidValue)
	ErrInvalid                      = errors.New("validation error")
	ErrInvalidType                  = fmt.Errorf("invalid type: %w", ErrInvalid)
	ErrInvalidRepositoryID          = fmt.Errorf("repository id: %w", ErrInvalidValue)
	ErrRequiredValue                = fmt.Errorf("required value: %w", ErrInvalid)
	ErrCommitNotFound               = fmt.Errorf("commit %w", ErrNotFound)
	ErrCreateBranchNoCommit         = fmt.Errorf("can't create a branch without commit")
	ErrRepositoryNotFound           = fmt.Errorf("repository %w", ErrNotFound)
	ErrRepositoryInDeletion         = errors.New("repository in deletion")
	ErrBranchNotFound               = fmt.Errorf("branch %w", ErrNotFound)
	ErrTagNotFound                  = fmt.Errorf("tag %w", ErrNotFound)
	ErrNoChanges                    = wrapError(ErrUserVisible, "no changes")
	ErrConflictFound                = wrapError(ErrUserVisible, "conflict found")
	ErrBranchExists                 = fmt.Errorf("branch already exists: %w", ErrNotUnique)
	ErrTagAlreadyExists             = fmt.Errorf("tag already exists: %w", ErrNotUnique)
	ErrDirtyBranch                  = wrapError(ErrUserVisible, "uncommitted changes (dirty branch)")
	ErrMetaRangeNotFound            = errors.New("metarange not found")
	ErrLockNotAcquired              = errors.New("lock not acquired")
	ErrRevertMergeNoParent          = wrapError(ErrUserVisible, "must specify 1-based parent number for reverting merge commit")
	ErrAddCommitNoParent            = errors.New("added commit must have a parent")
	ErrMultipleParents              = errors.New("cannot have more than a single parent")
	ErrRevertParentOutOfRange       = errors.New("given commit does not have the given parent number")
	ErrDereferenceCommitWithStaging = wrapError(ErrUserVisible, "reference to staging area with $ is not a commit")
	ErrDeleteDefaultBranch          = wrapError(ErrUserVisible, "cannot delete repository default branch")
	ErrCommitMetaRangeDirtyBranch   = wrapError(ErrUserVisible, "cannot use source MetaRange on a branch with uncommitted changes")
	ErrTooManyTries                 = errors.New("too many tries")
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

// DeleteError single delete error used by DeleteBatch's multierror.Error to report each key that failed
type DeleteError struct {
	Key Key
	Err error
}

func (d *DeleteError) Error() string {
	return fmt.Sprintf("%s: %s", d.Key, d.Err.Error())
}

func (d *DeleteError) Unwrap() error {
	return d.Err
}

// NewMapDeleteErrors map multi error holding DeleteError to a map of object key -> error
func NewMapDeleteErrors(err error) map[string]error {
	if err == nil {
		return nil
	}
	m := make(map[string]error)
	if merr, ok := err.(*multierror.Error); ok {
		for i := range merr.Errors {
			var delErr *DeleteError
			if errors.As(merr.Errors[i], &delErr) {
				m[string(delErr.Key)] = delErr.Err
			}
		}
	}
	return m
}
