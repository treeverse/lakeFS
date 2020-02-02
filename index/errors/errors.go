package errors

import "golang.org/x/xerrors"

var (
	ErrIndexMalformed             = xerrors.New("index error")
	ErrBadBlock                   = xerrors.New("block error")
	ErrMultipartPathMismatch      = xerrors.New("invalid path for multipart upload")
	ErrMultipartInvalidPartNumber = xerrors.New("invalid part number for multipart upload")
	ErrMultipartInvalidPartETag   = xerrors.New("invalid ETag for multipart upload")

	//validation errors
	ErrInvalid           = xerrors.New("validation error")
	ErrInvalidBucketName = xerrors.Errorf("bucket : %w", ErrInvalid)
)
