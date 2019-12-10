package block

import "io"

type ReadAtCloser interface {
	io.Reader
	io.ReaderAt
	io.Closer
}

type Adapter interface {
	Put(identifier string) (io.WriteCloser, error)
	Get(identifier string) (ReadAtCloser, error)
}
