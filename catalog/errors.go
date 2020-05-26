package catalog

import (
	"errors"
)

var (
	ErrIndexMalformed             = errors.New("index error")
	ErrMultipartPathMismatch      = errors.New("invalid path for multipart upload")
	ErrMultipartInvalidPartNumber = errors.New("invalid part number for multipart upload")
	ErrMultipartInvalidPartETag   = errors.New("invalid ETag for multipart upload")
	ErrRepoExists                 = errors.New("repository already exists")
	ErrBranchNotFound             = errors.New("branch not found")
	ErrBranchAlreadyExists        = errors.New("branch already exists")
	ErrNoMergeBase                = errors.New("no common merge base found")
	ErrDestinationNotCommitted    = errors.New("destination branch has uncommitted changes")
	ErrMergeConflict              = errors.New("merge conflict")
	ErrMergeUpdateFailed          = errors.New("failed updating merged destination")
	ErrInvalid                    = errors.New("validation error")
)
