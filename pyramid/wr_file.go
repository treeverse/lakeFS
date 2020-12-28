package pyramid

import (
	"errors"
	"fmt"
	"os"
)

// WRFile pyramid wrapper for os.file that triggers pyramid hooks for file actions.
type WRFile struct {
	*os.File

	persisted bool
	store     func(string) error
	abort     func() error
	aborted   bool
}

var (
	errFilePersisted = errors.New("file is persisted")
	errFileAborted   = errors.New("file is aborted")
)

// Store copies the closed file to all tiers of the pyramid.
func (f *WRFile) Store(filename string) error {
	if f.aborted {
		return errFileAborted
	}
	if f.persisted {
		return errFilePersisted
	}
	f.persisted = true

	if err := validateFilename(filename); err != nil {
		return err
	}

	if err := f.idempotentClose(); err != nil {
		return err
	}

	return f.store(filename)
}

// Abort delete the file and cleans all traces of it.
// If file was already stored, returns an error.
func (f *WRFile) Abort() error {
	if f.persisted {
		return errFilePersisted
	}
	f.aborted = true

	if err := f.idempotentClose(); err != nil {
		return err
	}

	return f.abort()
}

// idempotentClose is like Close(), but doesn't fail when the file is already closed.
func (f *WRFile) idempotentClose() error {
	err := f.File.Close()
	if err != nil && !errors.Is(err, os.ErrClosed) {
		return fmt.Errorf("closing file: %w", err)
	}
	return nil
}
