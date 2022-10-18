package graveler

import (
	"bytes"
)

// CombinedIterator iterates over two listing iterators,
// in case of duplication (in values or in errors) returns value in iterA.
// CombinedIterator can be constructed from other CombinedIterator to allow
// chaining of multiple ValueIterators.
type CombinedIterator struct {
	iterA ValueIterator
	iterB ValueIterator
	p     ValueIterator
}

// NewCombinedIterator combines multiple ValueIterators into a single CombinedIterator.
// The returned iterator precedence order is first-to-last in case of matching keys in 2 or more iterators.
func NewCombinedIterator(iters ...ValueIterator) ValueIterator {
	if len(iters) == 0 {
		panic("at least one iterator is required")
	}
	if len(iters) == 1 {
		return iters[0]
	}
	iter := newCombinedIterator(iters[0], iters[1])
	for i := 2; i < len(iters); i++ {
		iter = newCombinedIterator(iter, iters[i])
	}

	return iter
}

func newCombinedIterator(iterA, iterB ValueIterator) *CombinedIterator {
	return &CombinedIterator{
		iterA: iterA,
		iterB: iterB,
		p:     nil,
	}
}

// advanceInnerIterators advances the inner iterators and returns true if it has more
func (c *CombinedIterator) advanceInnerIterators() bool {
	valA := c.iterA.Value()
	valB := c.iterB.Value()
	var nextA bool
	var nextB bool
	switch {
	case c.p == nil:
		// first
		nextA = c.iterA.Next()
		nextB = c.iterB.Next()
	case valA == nil && valB == nil:
		// last
		return false
	case valA == nil:
		// iterA is done
		c.p = c.iterB
		nextB = c.iterB.Next()
	case valB == nil:
		// iterB is done
		c.p = c.iterA
		nextA = c.iterA.Next()
	case bytes.Equal(valA.Key, valB.Key):
		nextA = c.iterA.Next()
		nextB = c.iterB.Next()
	case bytes.Compare(valA.Key, valB.Key) < 0:
		nextA = c.iterA.Next()
		nextB = true
	default:
		// value of iterA < value of iterB
		nextB = c.iterB.Next()
		nextA = true
	}
	if c.iterA.Err() != nil {
		c.p = c.iterA
		return false
	}
	if c.iterB.Err() != nil {
		c.p = c.iterB
		return false
	}
	return nextA || nextB
}

func (c *CombinedIterator) Next() bool {
	if !c.advanceInnerIterators() {
		return false
	}
	// set c.p to be the next (smaller) value
	valA := c.iterA.Value()
	valB := c.iterB.Value()
	switch {
	case valA == nil && valB == nil:
		c.p = c.iterA
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

func (c *CombinedIterator) SeekGE(id Key) {
	c.p = nil
	c.iterA.SeekGE(id)
	c.iterB.SeekGE(id)
}

func (c *CombinedIterator) Value() *ValueRecord {
	if c.p == nil {
		return nil
	}
	return c.p.Value()
}

func (c *CombinedIterator) Err() error {
	if c.p == nil {
		return nil
	}
	return c.p.Err()
}

func (c *CombinedIterator) Close() {
	c.iterA.Close()
	c.iterB.Close()
}

// FilterTombstoneIterator wraps a value iterator and filters out tombstones.
type FilterTombstoneIterator struct {
	iter ValueIterator
}

func NewFilterTombstoneIterator(iter ValueIterator) *FilterTombstoneIterator {
	return &FilterTombstoneIterator{iter: iter}
}

func (f *FilterTombstoneIterator) Next() bool {
	for f.iter.Next() {
		if !f.iter.Value().IsTombstone() {
			return true
		}
	}
	return false
}

func (f *FilterTombstoneIterator) SeekGE(id Key) {
	f.iter.SeekGE(id)
}

func (f *FilterTombstoneIterator) Value() *ValueRecord {
	return f.iter.Value()
}

func (f *FilterTombstoneIterator) Err() error {
	return f.iter.Err()
}

func (f *FilterTombstoneIterator) Close() {
	f.iter.Close()
}
