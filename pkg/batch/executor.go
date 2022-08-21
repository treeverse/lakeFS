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

type BatchTracker interface {
	// Batched is called when a request is added to an existing batch.
	Batched()
}

type BatchFn func() (interface{}, error)

func (b BatchFn) Execute() (interface{}, error) {
	return b()
}

type DelayFn func(dur time.Duration)

type Batcher interface {
	BatchFor(ctx context.Context, key string, dur time.Duration, exec Executer) (interface{}, error)
}

type nonBatchingExecutor struct{}

// contextKey used to keep values on context.Context
type contextKey string

// SkipBatchContextKey existence on a context will eliminate the request batching
const SkipBatchContextKey contextKey = "skip_batch"

func (n *nonBatchingExecutor) BatchFor(_ context.Context, _ string, _ time.Duration, exec Executer) (interface{}, error) {
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

type Executor struct {
	// requests is the channel accepting inbound requests
	requests chan *request
	// execs is the internal channel used to dispatch the callback functions.
	// Several requests with the same key in a given duration will trigger a single write to exec said key.
	execs        chan string
	waitingOnKey map[string][]*request
	Logger       logging.Logger
	Delay        DelayFn
}

func NopExecutor() *nonBatchingExecutor {
	return &nonBatchingExecutor{}
}

func NewExecutor(logger logging.Logger) *Executor {
	return &Executor{
		requests:     make(chan *request, RequestBufferSize),
		execs:        make(chan string, RequestBufferSize),
		waitingOnKey: make(map[string][]*request),
		Logger:       logger,
		Delay:        time.Sleep,
	}
}

func (e *Executor) BatchFor(_ context.Context, key string, timeout time.Duration, exec Executer) (interface{}, error) {
	cb := make(chan *response)
	e.requests <- &request{
		key:        key,
		timeout:    timeout,
		exec:       exec,
		onResponse: cb,
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
			if _, exists := e.waitingOnKey[req.key]; !exists {
				e.waitingOnKey[req.key] = []*request{req}
				// this is a new key, let's fire a timer for it
				go func(req *request) {
					e.Delay(req.timeout)
					e.execs <- req.key
				}(req)
			} else {
				if b, ok := req.exec.(BatchTracker); ok {
					b.Batched()
				}
				e.waitingOnKey[req.key] = append(e.waitingOnKey[req.key], req)
			}
		case execKey := <-e.execs:
			// let's take all callbacks
			waiters := e.waitingOnKey[execKey]
			delete(e.waitingOnKey, execKey)
			go func(key string) {
				// execute and call all mapped callbacks
				v, err := waiters[0].exec.Execute()
				if e.Logger.IsTracing() {
					e.Logger.WithFields(logging.Fields{
						"waiters": len(waiters),
						"key":     key,
					}).Trace("dispatched BatchFn")
				}
				for _, waiter := range waiters {
					waiter.onResponse <- &response{v, err}
				}
			}(execKey)
		}
	}
}
