package batch

import (
	"time"

	"github.com/treeverse/lakefs/pkg/logging"
)

type BatchFn func() (interface{}, error)

type Batcher interface {
	BatchFor(key string, dur time.Duration, fn BatchFn) (interface{}, error)
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
	logger   logging.Logger
}

func NewExecutor(logger logging.Logger) *Executor {
	e := &Executor{
		requests: make(chan *request),
		execs:    make(chan string),
		keys:     make(map[string][]*request),
		logger:   logger,
	}
	go e.Run() // TODO(ozkatz): should probably be managed by the user (also, allow stopping it)
	return e
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

func (e *Executor) Run() {
	for {
		select {
		case req := <-e.requests:
			// see if we have it scheduled already
			if _, exists := e.keys[req.key]; !exists {
				// this is a new key, let's fire a timer for it
				go func(req *request) {
					time.Sleep(req.dur)
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
				e.logger.WithFields(logging.Fields{
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
