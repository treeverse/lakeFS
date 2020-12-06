package pyramid

import (
	"fmt"
	"os"
)

// File is pyramid wrapper for os.file that triggers pyramid hooks for file actions.
type File struct {
	*os.File

	closed    bool
	persisted bool
	store     func(string) error
}

func (f *File) Close() error {
	f.closed = true
	return f.File.Close()
}

var (
	errAlreadyPersisted = fmt.Errorf("file is already persisted")
	errFileNotClosed    = fmt.Errorf("file isn't closed")
)

// Store copies the closed file to all tiers of the pyramid.
func (f *File) Store(filename string) error {
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
