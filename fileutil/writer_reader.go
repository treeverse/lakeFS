package fileutil

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

// WriterThenReader writes data to storage, then allows reading it.  It is suitable for
// repeatedly processing large volumes of data.
type WriterThenReader interface {
	io.Writer
	// StartReading stops writing and returns a ResetableReader that will allow repeatedly
	// reading the data and the total length of data.  The WriterThenReader should be
	// discarded; calls to Write() after StartReading() will fail.
	StartReading() (RewindableReader, int64, error)
	// Name() returns a user-visible name for underlying storage.  It may help debug some
	// issues.
	Name() string
}

// RewindableReader allows repeatedly reading the same stream.
type RewindableReader interface {
	io.ReadSeeker
	// Rewind allows sets ResetableReader to start re-reading the same data.
	Rewind() error
	// Name() returns a user-visible name for underlying storage.  It may help debug some
	// issues.
	Name() string
}

type fileWriterThenReader struct{ file *os.File }

func NewFileWriterThenReader(basename string) (WriterThenReader, error) {
	file, err := ioutil.TempFile("", basename)
	if err != nil {
		return nil, fmt.Errorf("creating temporary file: %w", err)
	}
	if err = os.Remove(file.Name()); err != nil {
		return nil, fmt.Errorf("removing file %s from directory: %w", file.Name(), err)
	}
	return fileWriterThenReader{file: file}, nil
}

func (f fileWriterThenReader) Write(p []byte) (int, error) {
	return f.file.Write(p)
}

func (f fileWriterThenReader) StartReading() (RewindableReader, int64, error) {
	offset, err := f.file.Seek(0, os.SEEK_END)
	if err != nil {
		return fileRewindableReader{}, -1, err
	}
	_, err = f.file.Seek(0, os.SEEK_SET)
	ret := fileRewindableReader{file: f.file}
	// Break future attempts to use f
	f.file = nil
	return ret, offset, err
}

func (f fileWriterThenReader) Name() string { return f.file.Name() }

type fileRewindableReader struct{ file *os.File }

func (f fileRewindableReader) Read(p []byte) (n int, err error) {
	return f.file.Read(p)
}

func (f fileRewindableReader) Seek(offset int64, whence int) (int64, error) {
	return f.file.Seek(offset, whence)
}

func (f fileRewindableReader) Rewind() error {
	_, err := f.file.Seek(0, os.SEEK_SET)
	return err
}

func (f fileRewindableReader) Name() string { return f.file.Name() }
