package graveler

import (
	"errors"
	"fmt"
)

// Graveler errors
var (
	ErrNotFound                = errors.New("not found")
	ErrNotUnique               = errors.New("not unique")
	ErrInvalidValue            = errors.New("invalid value")
	ErrInvalidMergeBase        = fmt.Errorf("only 2 commits allowed in FindMergeBase: %w", ErrInvalidValue)
	ErrInvalidStorageNamespace = fmt.Errorf("storage namespace: %w", ErrInvalidValue)
	ErrInvalidRepositoryID     = fmt.Errorf("repository id: %w", ErrInvalidValue)
	ErrInvalidBranchID         = fmt.Errorf("branch id: %w", ErrInvalidValue)
	ErrInvalidRef              = fmt.Errorf("ref: %w", ErrInvalidValue)
	ErrInvalidCommitID         = fmt.Errorf("commit id: %w", ErrInvalidValue)
	ErrCommitNotFound          = fmt.Errorf("commit: %w", ErrNotFound)
	ErrRefAmbiguous            = fmt.Errorf("reference is ambiguous: %w", ErrNotFound)
	ErrConflictFound           = errors.New("conflict found")
	ErrBranchExists            = errors.New("branch already exists")
	ErrBranchLocked            = errors.New("branch locked for updates")
	ErrTagAlreadyExists        = errors.New("tag already exists")
	ErrDirtyBranch             = errors.New("can't apply meta-range on dirty branch")
	ErrMetaRangeNotFound       = errors.New("metarange not found")
)
