package errors

import (
	"github.com/treeverse/lakefs/db"
	"golang.org/x/xerrors"
)

var (
	ErrIndexMalformed             = xerrors.New("index error")
	ErrMultipartPathMismatch      = xerrors.New("invalid path for multipart upload")
	ErrMultipartInvalidPartNumber = xerrors.New("invalid part number for multipart upload")
	ErrMultipartInvalidPartETag   = xerrors.New("invalid ETag for multipart upload")
	ErrRepoExists                 = xerrors.New("repository already exists")
	ErrBranchNotFound             = xerrors.Errorf("branch : %w", db.ErrNotFound)
	ErrBranchAlreadyExists        = xerrors.New("branch already exists")
	ErrNoMergeBase                = xerrors.New("no common merge base found")
	ErrDestinationNotCommitted    = xerrors.New("destination branch not committed before merge")
	ErrMergeConflict              = xerrors.New("Merge Conflict")
	ErrMergeUpdateFailed          = xerrors.New("failed updating merged destinationt")

	//validation errors
	ErrInvalid = xerrors.New("validation error")
)
