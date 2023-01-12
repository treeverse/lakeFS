package block

import "github.com/pkg/errors"

var (
	ErrDataNotFound          = errors.New("not found")
	ErrOperationNotSupported = errors.New("operation not supported")
)
