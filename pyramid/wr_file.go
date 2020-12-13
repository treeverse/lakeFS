package pyramid

import (
	"fmt"
	"os"
)

// WRFile is pyramid wrapper for os.file that triggers pyramid hooks for file actions.
type WRFile struct {
	fh *os.File

	closed    bool
	persisted bool
	store     func(string) error
}

func (f *WRFile) Read(p []byte) (n int, err error) {
	return f.fh.Read(p)
}

func (f *WRFile) ReadAt(p []byte, off int64) (n int, err error) {
	return f.fh.ReadAt(p, off)
}

func (f *WRFile) Write(p []byte) (n int, err error) {
	return f.fh.Write(p)
}

func (f *WRFile) Stat() (os.FileInfo, error) {
	return f.fh.Stat()
}

func (f *WRFile) Sync() error {
	return f.fh.Sync()
}

func (f *WRFile) Close() error {
	f.closed = true
	return f.fh.Close()
}

var (
	errAlreadyPersisted = fmt.Errorf("file is already persisted")
	errFileNotClosed    = fmt.Errorf("file isn't closed")
)

// Store copies the closed file to all tiers of the pyramid.
func (f *WRFile) Store(filename string) error {
	if err := validateFilename(filename); err != nil {
		return err
	}

	if f.persisted {
		return errAlreadyPersisted
	}
	if !f.closed {
		return errFileNotClosed
	}

	err := f.store(filename)
	if err == nil {
		f.persisted = true
	}
	return err
}
