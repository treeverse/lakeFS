package esti

import "io"

type WrappedReader struct {
	reader       io.Reader
	numBytesRead int
}

func NewWrappedReader(reader io.Reader) *WrappedReader {
	return &WrappedReader{
		reader:       reader,
		numBytesRead: 0,
	}
}

func (w *WrappedReader) Read(p []byte) (n int, err error) {
	n, err = w.reader.Read(p)
	w.numBytesRead += n
	return n, err
}
