package graveler

import "bytes"

// combinedIterator iterates over two listing iterators,
// in case of duplication (in values or in errors) returns value in iterA
type combinedIterator struct {
	iterA ValueIterator
	iterB ValueIterator
	p     ValueIterator
}

func NewCombinedIterator(iterA, iterB ValueIterator) ValueIterator {
	return &combinedIterator{
		iterA: iterA,
		iterB: iterB,
		p:     nil,
	}
}

func (c *combinedIterator) Next() bool {
	// call next with the relevant iterators
	valA := c.iterA.Value()
	valB := c.iterB.Value()

	switch {
	case c.p == nil:
		// first
		c.iterA.Next()
		c.iterB.Next()
	case valA == nil && valB == nil:
		// last
		return false
	case valA == nil:
		c.p = c.iterB
		return c.iterB.Next()
	case valB == nil:
		c.p = c.iterA
		return c.iterA.Next()
	case bytes.Equal(valA.Key, valB.Key):
		c.iterA.Next()
		c.iterB.Next()
	case bytes.Compare(valA.Key, valB.Key) < 0:
		c.iterA.Next()
	default:
		// value of iterA < value of iterB
		c.iterB.Next()
	}

	if c.iterA.Err() != nil {
		c.p = c.iterA
		return false
	}
	if c.iterB.Err() != nil {
		c.p = c.iterB
		return false
	}
	// get the current pointer
	valA = c.iterA.Value()
	valB = c.iterB.Value()
	switch {
	case valA == nil && valB == nil:
		c.p = c.iterA // in order not to be stuck in start state
		return false
	case valA == nil:
		c.p = c.iterB
	case valB == nil:
		c.p = c.iterA
	case bytes.Compare(valA.Key, valB.Key) <= 0:
		c.p = c.iterA
	default:
		c.p = c.iterB
	}
	return true
}

func (c *combinedIterator) SeekGE(id Key) {
	c.p = nil
	c.iterA.SeekGE(id)
	c.iterB.SeekGE(id)
}

func (c *combinedIterator) Value() *ValueRecord {
	if c.p == nil {
		return nil
	}
	return c.p.Value()
}

func (c *combinedIterator) Err() error {
	if c.p == nil {
		return nil
	}
	return c.p.Err()
}

func (c *combinedIterator) Close() {
	c.iterA.Close()
	c.iterB.Close()
}
