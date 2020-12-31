package graveler

import (
	"bytes"

	"github.com/treeverse/lakefs/logging"
)

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

// advanceInnerIterators advances the inner iterators and returns true if has more
func (c *combinedIterator) advanceInnerIterators() bool {
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

func (c *combinedIterator) Next() bool {
	for {
		if !c.advanceInnerIterators() {
			return false
		}
		// set c.p to be the next (smaller) value or continue in case of tombstone
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
		case bytes.Equal(valA.Key, valB.Key) && valA.IsTombstone():
			// continue without tombstone error
			continue
		case bytes.Compare(valA.Key, valB.Key) <= 0:
			c.p = c.iterA
		default:
			c.p = c.iterB
		}
		if c.p.Value().IsTombstone() {
			logging.Default().Error("unexpected tombstone")
			continue
		}
		return true
	}
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
