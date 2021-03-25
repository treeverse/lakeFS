package cache

import "sync"

// OnlyOne ensures only one concurrent evaluation of a keyed expression.
type OnlyOne interface {
	// Compute returns the value of calling fn(), but only calls fn once concurrently for
	// each k.
	Compute(k interface{}, fn func() (interface{}, error)) (interface{}, error)
}

type ChanOnlyOne struct {
	m *sync.Map
}

func NewChanOnlyOne() *ChanOnlyOne {
	return &ChanOnlyOne{
		m: &sync.Map{},
	}
}

type chanAndResult struct {
	ch    chan struct{}
	value interface{}
	err   error
}

func (c *ChanOnlyOne) Compute(k interface{}, fn func() (interface{}, error)) (interface{}, error) {
	stop := chanAndResult{ch: make(chan struct{})}
	actual, inFlight := c.m.LoadOrStore(k, &stop)
	actualStop := actual.(*chanAndResult)
	if inFlight {
		<-actualStop.ch
	} else {
		actualStop.value, actualStop.err = fn()
		close(actualStop.ch)
		c.m.Delete(k)
	}
	return actualStop.value, actualStop.err
}
