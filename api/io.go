package api

import "io"

type MultiReadCloser struct {
	closers []io.ReadCloser
	reader  io.Reader
}

func NewMultiReadCloser(closeReaders []io.ReadCloser) MultiReadCloser {
	readers := make([]io.Reader, len(closeReaders))
	for i, r := range closeReaders {
		readers[i] = r
	}
	return MultiReadCloser{
		closers: closeReaders,
		reader:  io.MultiReader(readers...),
	}
}

func (m MultiReadCloser) Read(p []byte) (int, error) {
	return m.reader.Read(p)
}

func (m MultiReadCloser) Close() error {
	for _, r := range m.closers {
		err := r.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
