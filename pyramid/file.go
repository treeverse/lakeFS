package pyramid

import (
	"errors"
	"fmt"
	"os"
)

// File is pyramid wrapper for os.file that triggers pyramid hooks for file actions.
type File struct {
	*os.File

	persisted bool
	store     func(string) error
}

var (
	errAlreadyPersisted = fmt.Errorf("file is already persisted")
)

// Store copies the closed file to all tiers of the pyramid.
func (f *File) Store(filename string) error {
	if err := validateFilename(filename); err != nil {
		return err
	}

	err := f.File.Close()
	if err != nil && !errors.Is(err, os.ErrClosed) {
		return fmt.Errorf("closing file: %w", err)
	}

	if f.persisted {
		return errAlreadyPersisted
	}

	err = f.store(filename)
	if err == nil {
		f.persisted = true
	}
	return err
}
