package sstable

import (
	"errors"
	"sync"

	"github.com/treeverse/lakefs/graveler/committed"

	"go.uber.org/atomic"
)

type BatchCloser struct {
	results []committed.WriteResult
	err     atomic.Error

	wg sync.WaitGroup

	// resultsLock locks any access to the accumulated results
	resultsLock sync.Mutex
}

var (
	errMultipleWaitCalls = errors.New("wait has already been called")
)

// CloseWriterAsync adds Writer instance for the BatchWriterCloser to handle.
// Any writes executed to the writer after this call are not guaranteed to succeed.
// If Wait() has already been called, returns an error.
func (bc *BatchCloser) CloseWriterAsync(w committed.Writer) error {
	err := bc.err.Load()
	if err != nil {
		// don't accept new writers if previous error occurred
		return err
	}

	bc.wg.Add(1)
	go bc.closeWriter(w)

	return nil
}

func (bc *BatchCloser) closeWriter(w committed.Writer) {
	defer bc.wg.Done()
	res, err := w.Close()
	if err != nil {
		if bc.err.Load() == nil {
			// keeping first error is enough
			bc.err.Store(err)
		}
		return
	}

	bc.resultsLock.Lock()
	defer bc.resultsLock.Unlock()
	bc.results = append(bc.results, *res)
}

// Wait returns when all Writers finished.
// Any failure to close a single Writer will return with a nil results slice and an error.
func (bc *BatchCloser) Wait() ([]committed.WriteResult, error) {
	err := bc.err.Load()
	if err != nil {
		return nil, err
	}
	bc.err.Store(errMultipleWaitCalls)

	return bc.results, nil
}
