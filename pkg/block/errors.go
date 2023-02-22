package block

import "github.com/pkg/errors"

var (
	ErrDataNotFound          = errors.New("not found")
	ErrOperationNotSupported = errors.New("operation not supported")
	ErrAsyncCopyFailed       = errors.New("async copy failed")
	ErrBadIndex              = errors.New("bad index")
)
