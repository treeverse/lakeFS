package stats

import (
	"context"
	"fmt"
	"time"
)

const CollectorEventBufferSize = 1024 * 1024

type primaryKey struct {
	class string
	name  string
}

func (p primaryKey) String() string {
	return fmt.Sprintf("%s/%s", p.class, p.name)
}

type keyIndex map[primaryKey]uint64

type flushRequest struct {
	flushed chan keyIndex
}

type Collector interface {
	Collect(class, action string)
}

type CallHomeCollector struct {
	ctx           context.Context
	stop          context.CancelFunc
	cache         keyIndex
	writes        chan primaryKey
	flushes       chan flushRequest
	flushInterval time.Duration
	drained       chan bool
}

type CollectorOpt func(c *CallHomeCollector)

func WriteBufferSize(bufSize int) CollectorOpt {
	return func(c *CallHomeCollector) {
		c.writes = make(chan primaryKey, bufSize)
	}
}

func NewCallHomeCollector(opts ...CollectorOpt) *CallHomeCollector {
	ctx, stop := context.WithCancel(context.Background())
	collector := &CallHomeCollector{
		ctx:     ctx,
		stop:    stop,
		cache:   make(keyIndex),
		writes:  make(chan primaryKey, CollectorEventBufferSize),
		flushes: make(chan flushRequest),
		drained: make(chan bool),
	}
	for _, opt := range opts {
		opt(collector)
	}
	return collector
}

func (c *CallHomeCollector) Collect(class, action string) {
	pk := primaryKey{
		class: class,
		name:  action,
	}
	c.writes <- pk
}

func (c *CallHomeCollector) incr(pk primaryKey) {
	if currentValue, exists := c.cache[pk]; exists {
		c.cache[pk] = currentValue + 1
	} else {
		c.cache[pk] = 1
	}
}

func (c *CallHomeCollector) flushCounters(r flushRequest) {
	flushed := make(keyIndex)   // create a new map to copy values into
	for k, v := range c.cache { // copy all current cache values
		flushed[k] = v
	}
	r.flushed <- flushed     // write new map back to the request channel
	c.cache = make(keyIndex) // replace cache with a new empty map
}

func (c *CallHomeCollector) Flush() keyIndex {
	r := flushRequest{make(chan keyIndex)}
	c.flushes <- r     // ask loop to flushes counters
	return <-r.flushed // read and return flushed counters
}

func (c *CallHomeCollector) Drain() keyIndex {
	c.stop()       // trigger the run loop to stop
	<-c.drained    // wait until the drained signal is received
	return c.cache // return remaining counts if any
}

// read everything buffered in the writes channel, blocking until empty
func (c *CallHomeCollector) drainWrites() {
	for {
		select {
		case pk := <-c.writes:
			c.incr(pk)
		default:
			return
		}
	}
}

// Run waits for write and flush requests on the appropriate channels
func (c *CallHomeCollector) Run() {
	for {
		select {
		case <-c.ctx.Done(): // got a signal telling us to stop
			c.drainWrites()   // drain all remaining writes on the writes channel
			c.drained <- true // signal that we're drained and will no longer poll for writes
			return
		case fr := <-c.flushes:
			c.flushCounters(fr)
		case pk := <-c.writes:
			c.incr(pk)
		}
	}
}
