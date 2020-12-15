package sstable

import (
	"errors"
	"sync"

	"github.com/treeverse/lakefs/graveler/committed"
)

type BatchCloser struct {
	results []committed.WriteResult
	err     error

	wg sync.WaitGroup

	// lock locks any access to the results and error
	lock sync.Mutex
}

// NewBatchCloser returns a new BatchCloser
func NewBatchCloser() *BatchCloser {
	return &BatchCloser{
		wg:   sync.WaitGroup{},
		lock: sync.Mutex{},
	}
}

var (
	errMultipleWaitCalls = errors.New("wait has already been called")
)

// CloseWriterAsync adds Writer instance for the BatchWriterCloser to handle.
// Any writes executed to the writer after this call are not guaranteed to succeed.
// If Wait() has already been called, returns an error.
func (bc *BatchCloser) CloseWriterAsync(w committed.Writer) error {
	bc.lock.Lock()
	defer bc.lock.Unlock()

	if bc.err != nil {
		// don't accept new writers if previous error occurred
		return bc.err
	}

	bc.wg.Add(1)
	go bc.closeWriter(w)

	return nil
}

func (bc *BatchCloser) closeWriter(w committed.Writer) {
	defer bc.wg.Done()
	res, err := w.Close()

	// long operation is over, we can lock to have synchronized access to err and results
	bc.lock.Lock()
	defer bc.lock.Unlock()

	if err != nil {
		if bc.nilErrOrMultipleCalls() {
			// keeping first error is enough
			bc.err = err
		}
		return
	}

	bc.results = append(bc.results, *res)
}

// Wait returns when all Writers finished.
// Any failure to close a single Writer will return with a nil results slice and an error.
func (bc *BatchCloser) Wait() ([]committed.WriteResult, error) {
	bc.lock.Lock()
	if bc.err != nil {
		defer bc.lock.Unlock()
		return nil, bc.err
	}
	bc.err = errMultipleWaitCalls
	bc.lock.Unlock()

	bc.wg.Wait()

	// all writers finished
	bc.lock.Lock()
	defer bc.lock.Unlock()
	if !bc.nilErrOrMultipleCalls() {
		return nil, bc.err
	}
	return bc.results, nil
}

func (bc *BatchCloser) nilErrOrMultipleCalls() bool {
	return bc.err == nil || errors.Is(bc.err, errMultipleWaitCalls)
}
