package testutil

import (
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/go.net/context"
)

type Request struct {
	Payload string
}

type Result struct {
	Error error
	Took  time.Duration
}

type WorkFn func(Request) error

func Execute(ctx context.Context, parallelism int, fn WorkFn, input chan Request, output chan Result) {
	// spawn workers
	worker := func(ctx context.Context, fn WorkFn, input chan Request, output chan Result) {
		for {
			select {
			case request := <-input:
				start := time.Now()
				err := fn(request)
				output <- Result{
					Error: err,
					Took:  time.Since(start),
				}
			case <-ctx.Done():
				return
			}
		}
	}
	for i := 0; i < parallelism; i++ {
		go worker(ctx, fn, input, output)
	}

	<-ctx.Done() // block until done
}

type Histogram struct {
	buckets  []int64
	counters map[int64]int64
}

func NewHistogram(buckets []int64) *Histogram {
	return &Histogram{
		buckets:  buckets,
		counters: make(map[int64]int64),
	}
}

func (h *Histogram) String() string {
	builder := &strings.Builder{}
	for _, b := range h.buckets {
		builder.WriteString(fmt.Sprintf("%d\t%d\n", b, h.counters[b]))
	}
	return builder.String()
}

func (h *Histogram) Add(v int64) {
	for _, b := range h.buckets {
		if v < b {
			h.counters[b]++
		}
	}
}

type ResultCollector struct {
	histogram *Histogram
	totals    int64
	current   int64
	errors    int64

	input chan Result

	emitEvery time.Duration
	emitChan  chan string
}

func (rc *ResultCollector) Sink() chan Result {
	return rc.input
}

func (rc *ResultCollector) flushCurrent() string {
	return fmt.Sprintf("completed: %d, errors: %d, current rate: %.2f done/second, histogram (ms):\n%s\n\n",
		rc.totals, rc.errors,
		float64(rc.current)/rc.emitEvery.Seconds(),
		rc.histogram)
}

func (rc *ResultCollector) Collect(ctx context.Context) {
	every := time.NewTicker(rc.emitEvery)
	for {
		select {
		case result := <-rc.input:
			rc.totals++
			rc.current++
			if result.Error != nil {
				rc.errors++
			} else {
				rc.histogram.Add(result.Took.Milliseconds())
			}
		case <-every.C:
			// write but don't block if no-one's listening.
			select {
			case rc.emitChan <- rc.flushCurrent():
			default:
			}

			rc.current = 0
		case <-ctx.Done():
			every.Stop()
			close(rc.emitChan)
			close(rc.input)
			return
		}
	}
}

func (rc *ResultCollector) EmitChan() chan string {
	return rc.emitChan
}

func NewResultCollector(emitEvery time.Duration) *ResultCollector {
	return &ResultCollector{
		histogram: NewHistogram([]int64{}),
		input:     make(chan Result),
		emitEvery: emitEvery,
		emitChan:  make(chan string),
	}
}
