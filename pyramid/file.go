package pyramid

import (
	"errors"
	"os"
)

// File is pyramid wrapper for os.file that triggers pyramid hooks for file actions.
type File struct {
	fh       *os.File
	eviction eviction

	readOnly bool
	rPath    relativePath

	store func() error
}

var ErrReadOnlyFile = errors.New("file is read-only")

func (f *File) Read(p []byte) (n int, err error) {
	if f.readOnly {
		// file is being written, eviction policies don't apply to it
		f.eviction.touch(f.rPath)
	}
	return f.fh.Read(p)
}

func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
	if f.readOnly {
		// file is being written, eviction policies don't apply to it
		f.eviction.touch(f.rPath)
	}
	return f.fh.ReadAt(p, off)
}

func (f *File) Write(p []byte) (n int, err error) {
	if f.readOnly {
		return 0, ErrReadOnlyFile
	}

	return f.fh.Write(p)
}

func (f *File) Stat() (os.FileInfo, error) {
	return f.fh.Stat()
}

func (f *File) Sync() error {
	return f.fh.Sync()
}

func (f *File) Close() error {
	if err := f.fh.Close(); err != nil {
		return err
	}

	if f.store == nil {
		return nil
	}
	return f.store()
}
