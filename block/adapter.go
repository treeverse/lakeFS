package block

import "io"

type Adapter interface {
	Put(repo string, identifier string, reader io.ReadSeeker) error
	Get(repo string, identifier string) (io.ReadCloser, error)
	GetRange(repo string, identifier string, startPosition int64, endPosition int64) (io.ReadCloser, error)
}
