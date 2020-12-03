package sstable

import (
	"errors"
	"sync"

	"go.uber.org/atomic"
)

type BatchCloser struct {
	results []WriteResult
	err     error

	wg          sync.WaitGroup
	waitCalled  atomic.Bool
	resultsLock sync.Mutex
}

// CloseWriterAsync adds Writer instance for the BatchWriterCloser to handle.
// Any writes executed to the writer after this call are not guaranteed to succeed.
// If Wait() has already been called, returns an error.
func (bc *BatchCloser) CloseWriterAsync(w Writer) error {
	if bc.err != nil {
		// don't accept new writers if previous error occurred
		return bc.err
	}
	if bc.waitCalled.Load() {
		return errors.New("wait has already been called")
	}

	bc.wg.Add(1)
	go bc.closeWriter(w)

	return nil
}

func (bc *BatchCloser) closeWriter(w Writer) {
	defer bc.wg.Done()
	res, err := w.Close()
	if err != nil {
		if bc.err != nil {
			// keeping first error is enough
			bc.err = err
		}
		return
	}

	bc.resultsLock.Lock()
	defer bc.resultsLock.Unlock()
	bc.results = append(bc.results, *res)
}

// Wait returns when all Writers finished.
// Any failure to close a single Writer will return with a nil results slice and an error.
func (bc *BatchCloser) Wait() ([]WriteResult, error) {
	bc.waitCalled.Store(true)
	bc.wg.Wait()
	if bc.err != nil {
		return nil, bc.err
	}

	return bc.results, nil
}
