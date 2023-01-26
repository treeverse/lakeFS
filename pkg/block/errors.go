package block

import "github.com/pkg/errors"

var (
	ErrDataNotFound          = errors.New("not found")
	ErrOperationNotSupported = errors.New("operation not supported")
	ErrAsyncCopyInfo         = errors.New("failed to get copy status")
	ErrAsyncCopyFailed       = errors.New("async copy failed")
	ErrAsyncCopyAborted      = errors.New("async copy unexpected abort")
)
