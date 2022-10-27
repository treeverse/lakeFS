package committed

import (
	"errors"
	"sync"
)

type ResultCloser interface {
	Close() (*WriteResult, error)
}

type BatchCloser struct {
	// mu protects results and error
	mu      sync.Mutex
	results []WriteResult
	err     error

	wg sync.WaitGroup
	ch chan ResultCloser
}

// NewBatchCloser returns a new BatchCloser
func NewBatchCloser(numClosers int) *BatchCloser {
	ret := &BatchCloser{
		// Block when all closer goroutines are busy.
		ch: make(chan ResultCloser),
	}

	ret.wg.Add(numClosers)
	for i := 0; i < numClosers; i++ {
		go ret.handleClose()
	}

	return ret
}

var ErrMultipleWaitCalls = errors.New("wait has already been called")

// CloseWriterAsync adds RangeWriter instance for the BatchCloser to handle.
// Any writes executed to the writer after this call are not guaranteed to succeed.
// If Wait() has already been called, returns an error.
func (bc *BatchCloser) CloseWriterAsync(w ResultCloser) error {
	bc.mu.Lock()

	if bc.err != nil {
		// Don't accept new writers if previous error occurred.
		// In particular, if Wait has started then this is errMultipleWaitCalls.
		bc.mu.Unlock()
		return bc.err
	}

	bc.mu.Unlock()
	bc.ch <- w

	return nil
}

func (bc *BatchCloser) handleClose() {
	for w := range bc.ch {
		bc.closeWriter(w)
	}
	bc.wg.Done()
}

func (bc *BatchCloser) closeWriter(w ResultCloser) {
	res, err := w.Close()

	// long operation is over, we can lock to have synchronized access to err and results
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if err != nil {
		if bc.nilErrOrMultipleCalls() {
			// keeping first error is enough
			bc.err = err
		}
		return
	}

	bc.results = append(bc.results, *res)
}

// Wait returns when all Writers finished.  Returns a nil results slice and an error if *any*
// RangeWriter failed to close and upload.
func (bc *BatchCloser) Wait() ([]WriteResult, error) {
	bc.mu.Lock()
	if bc.err != nil {
		defer bc.mu.Unlock()
		return nil, bc.err
	}
	bc.err = ErrMultipleWaitCalls
	bc.mu.Unlock()

	close(bc.ch)

	bc.wg.Wait()

	// all writers finished
	bc.mu.Lock()
	defer bc.mu.Unlock()
	if !bc.nilErrOrMultipleCalls() {
		return nil, bc.err
	}
	return bc.results, nil
}

func (bc *BatchCloser) nilErrOrMultipleCalls() bool {
	return bc.err == nil || errors.Is(bc.err, ErrMultipleWaitCalls)
}
