package batch

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/pkg/logging"
)

type BatchFn func() (interface{}, error)

type DelayFn func(dur time.Duration)

type Batcher interface {
	BatchFor(key string, dur time.Duration, fn BatchFn) (interface{}, error)
}

type nonBatchingExecutor struct {
}

func (n *nonBatchingExecutor) BatchFor(key string, dur time.Duration, fn BatchFn) (interface{}, error) {
	return fn()
}

type response struct {
	v   interface{}
	err error
}

type request struct {
	key              string
	dur              time.Duration
	fn               BatchFn
	responseCallback chan *response
}

type Executor struct {
	requests chan *request
	execs    chan string
	keys     map[string][]*request
	Logger   logging.Logger
	Delayer  DelayFn
}

func NopExecutor() *nonBatchingExecutor {
	return &nonBatchingExecutor{}
}

func NewExecutor(logger logging.Logger) *Executor {
	return &Executor{
		requests: make(chan *request),
		execs:    make(chan string),
		keys:     make(map[string][]*request),
		Logger:   logger,
		Delayer:  time.Sleep,
	}
}

func (e *Executor) BatchFor(key string, dur time.Duration, fn BatchFn) (interface{}, error) {
	cb := make(chan *response)
	e.requests <- &request{
		key:              key,
		dur:              dur,
		fn:               fn,
		responseCallback: cb,
	}
	response := <-cb
	return response.v, response.err
}

func (e *Executor) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-e.requests:
			// see if we have it scheduled already
			if _, exists := e.keys[req.key]; !exists {
				// this is a new key, let's fire a timer for it
				go func(req *request) {
					e.Delayer(req.dur)
					e.execs <- req.key
				}(req)
			}
			e.keys[req.key] = append(e.keys[req.key], req)
		case execKey := <-e.execs:
			// let's take all callbacks
			waiters := e.keys[execKey]
			delete(e.keys, execKey)
			go func(key string) {
				// execute and call all mapped callbacks
				v, err := waiters[0].fn()
				e.Logger.WithFields(logging.Fields{
					"waiters": len(waiters),
					"key":     key,
				}).Trace("dispatched BatchFn")
				for _, waiter := range waiters {
					waiter.responseCallback <- &response{v, err}
				}
			}(execKey)
		}
	}
}
