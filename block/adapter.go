package block

import (
	"context"
	"io"
)

type Adapter interface {
	WithContext(ctx context.Context) Adapter
	Put(repo string, identifier string, reader io.ReadSeeker) error
	Get(repo string, identifier string) (io.ReadCloser, error)
	GetRange(repo string, identifier string, startPosition int64, endPosition int64) (io.ReadCloser, error)
	Remove(repo string, identifier string) error
}
