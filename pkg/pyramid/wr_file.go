package pyramid

import (
	"context"
	"errors"
	"fmt"
	"os"
)

// WRFile pyramid wrapper for os.file that triggers pyramid hooks for file actions.
type WRFile struct {
	*os.File
	cancelStore context.CancelFunc

	persisted bool
	store     func(context.Context, string) error
	abort     func(context.Context) error
	aborted   bool
}

// Store copies the closed file to all tiers of the pyramid.
func (f *WRFile) Store(ctx context.Context, filename string) error {
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

	// keep the cancel function for the Store's context,
	// so that the long operation is cancellable during a call to Abort.
	ctx, f.cancelStore = context.WithCancel(ctx)
	return f.store(ctx, filename)
}

// Abort deletes the file and cleans all traces of it.
// If file was already stored, returns an error.
func (f *WRFile) Abort(ctx context.Context) error {
	if f.cancelStore != nil {
		// canceling the ongoing store operation
		defer f.cancelStore()
		f.cancelStore = nil
	}

	if f.persisted {
		return errFilePersisted
	}
	f.aborted = true

	if err := f.idempotentClose(); err != nil {
		return err
	}

	return f.abort(ctx)
}

// idempotentClose is like Close(), but doesn't fail when the file is already closed.
func (f *WRFile) idempotentClose() error {
	err := f.File.Close()
	if err != nil && !errors.Is(err, os.ErrClosed) {
		return fmt.Errorf("closing file: %w", err)
	}
	return nil
}
