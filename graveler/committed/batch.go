package committed

import (
	"errors"
	"sync"
)

type ResultCloser interface {
	Close() (*WriteResult, error)
}

type BatchCloser struct {
	results []WriteResult
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
	ErrMultipleWaitCalls = errors.New("wait has already been called")
)

// CloseWriterAsync adds RangeWriter instance for the BatchCloser to handle.
// Any writes executed to the writer after this call are not guaranteed to succeed.
// If Wait() has already been called, returns an error.
func (bc *BatchCloser) CloseWriterAsync(w ResultCloser) error {
	bc.lock.Lock()
	defer bc.lock.Unlock()

	if bc.err != nil {
		// Don't accept new writers if previous error occurred.
		// In particular, if Wait has started then this is errMultipleWaitCalls.
		return bc.err
	}

	bc.wg.Add(1)
	go bc.closeWriter(w)

	return nil
}

func (bc *BatchCloser) closeWriter(w ResultCloser) {
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
// Any failure to close a single RangeWriter will return with a nil results slice and an error.
func (bc *BatchCloser) Wait() ([]WriteResult, error) {
	bc.lock.Lock()
	if bc.err != nil {
		defer bc.lock.Unlock()
		return nil, bc.err
	}
	bc.err = ErrMultipleWaitCalls
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
	return bc.err == nil || errors.Is(bc.err, ErrMultipleWaitCalls)
}
