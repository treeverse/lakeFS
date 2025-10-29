package batch

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/pkg/logging"
)

// RequestBufferSize is the amount of requests users can dispatch that haven't been processed yet before
// dispatching new ones would start blocking.
const RequestBufferSize = 1 << 17

type Executer interface {
	Execute() (interface{}, error)
}

type ExecuterFunc func() (interface{}, error)

func (b ExecuterFunc) Execute() (interface{}, error) {
	return b()
}

type Tracker interface {
	// Batched is called when a request is added to an existing batch.
	Batched()
}

type DelayFn func(dur time.Duration)

type Batcher interface {
	BatchFor(ctx context.Context, key string, dur time.Duration, exec Executer) (interface{}, error)
}

type NoOpBatchingExecutor struct{}

// contextKey used to keep values on context.Context
type contextKey string

// SkipBatchContextKey existence on a context will eliminate the request batching
const SkipBatchContextKey contextKey = "skip_batch"

func (n *NoOpBatchingExecutor) BatchFor(_ context.Context, _ string, _ time.Duration, exec Executer) (interface{}, error) {
	return exec.Execute()
}

// ConditionalExecutor will batch requests only if SkipBatchContextKey is not on the context
// of the batch request.
type ConditionalExecutor struct {
	executor *Executor
}

func NewConditionalExecutor(logger logging.Logger) *ConditionalExecutor {
	return &ConditionalExecutor{executor: NewExecutor(logger)}
}

func (c *ConditionalExecutor) Run(ctx context.Context) {
	c.executor.Run(ctx)
}

func (c *ConditionalExecutor) BatchFor(ctx context.Context, key string, timeout time.Duration, exec Executer) (interface{}, error) {
	if ctx.Value(SkipBatchContextKey) != nil {
		return exec.Execute()
	}
	return c.executor.BatchFor(ctx, key, timeout, exec)
}

type response struct {
	v   interface{}
	err error
}

type request struct {
	key        string
	timeout    time.Duration
	exec       Executer
	onResponse chan *response
}

// Waiters holds waiting requests.
type Waiters struct {
	// ctx is the context associated with the first request.  It is
	// waiting for the longest time.
	ctx      context.Context
	requests []*request
}

// WaitFor holds a request and its original context.
type WaitFor struct {
	ctx     context.Context
	request *request
}

type Executor struct {
	// requests is the channel accepting inbound requests
	requests chan *WaitFor
	// execs is the internal channel used to dispatch the callback functions.
	// Several requests with the same key in a given duration will trigger a single write to exec said key.
	execs        chan string
	waitingOnKey map[string]*Waiters
	Logger       logging.Logger
	Delay        DelayFn
}

func NopExecutor() *NoOpBatchingExecutor {
	return &NoOpBatchingExecutor{}
}

func NewExecutor(logger logging.Logger) *Executor {
	return &Executor{
		requests:     make(chan *WaitFor, RequestBufferSize),
		execs:        make(chan string, RequestBufferSize),
		waitingOnKey: make(map[string]*Waiters),
		Logger:       logger,
		Delay:        time.Sleep,
	}
}

func (e *Executor) BatchFor(ctx context.Context, key string, timeout time.Duration, exec Executer) (interface{}, error) {
	cb := make(chan *response)
	e.requests <- &WaitFor{
		ctx: ctx,
		request: &request{
			key:        key,
			timeout:    timeout,
			exec:       exec,
			onResponse: cb,
		},
	}
	res := <-cb
	return res.v, res.err
}

func (e *Executor) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case waitFor := <-e.requests:
			req := waitFor.request
			// see if we have it scheduled already
			waiters, exists := e.waitingOnKey[req.key]
			if !exists {
				waiters = &Waiters{ctx: waitFor.ctx, requests: []*request{req}}
				e.waitingOnKey[req.key] = waiters
				// this is a new key, let's fire a timer for it
				go func(req *request) {
					e.Delay(req.timeout)
					e.execs <- req.key
				}(req)
			} else {
				if b, ok := req.exec.(Tracker); ok {
					b.Batched()
				}
				waiters.requests = append(waiters.requests, req)
			}
		case execKey := <-e.execs:
			// let's take all callbacks
			waiters := e.waitingOnKey[execKey]
			delete(e.waitingOnKey, execKey)
			go func(key string) {
				// execute and call all mapped callbacks
				v, err := waiters.requests[0].exec.Execute()
				if e.Logger.IsTracing() {
					e.Logger.
						WithContext(waiters.ctx).
						WithFields(logging.Fields{
							"waiters": len(waiters.requests),
							"key":     key,
						}).Trace("dispatched execute result")
				}
				for _, waiter := range waiters.requests {
					waiter.onResponse <- &response{v, err}
				}
			}(execKey)
		}
	}
}
