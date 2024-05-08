package graveler

import "bytes"

// JoinedDiffIterator calculate the union diff between 2 iterators.
// The output iterator yields a single result for each key.
// If a key exist in the 2 iterators, iteratorA value prevails.
type JoinedDiffIterator struct {
	iterA DiffIterator
	iterB DiffIterator
	p     DiffIterator
}

func NewJoinedDiffIterator(iterA DiffIterator, iterB DiffIterator) *JoinedDiffIterator {
	return &JoinedDiffIterator{
		iterA: iterA,
		iterB: iterB,
		p:     nil,
	}
}

// advanceInnerIterators advances the inner iterators and returns true if it has more
func (c *JoinedDiffIterator) advanceInnerIterators() bool {
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

func (c *JoinedDiffIterator) Next() bool {
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

func (c *JoinedDiffIterator) Value() *Diff {
	if c.p == nil {
		return nil
	}
	return c.p.Value()
}

func (c *JoinedDiffIterator) Err() error {
	if c.p != nil && c.p.Err() != nil {
		return c.p.Err()
	}
	if c.iterA != nil && c.iterA.Err() != nil {
		return c.iterA.Err()
	}
	if c.iterB != nil && c.iterB.Err() != nil {
		return c.iterB.Err()
	}
	return nil
}

func (c *JoinedDiffIterator) Close() {
	c.iterA.Close()
	c.iterB.Close()
}

func (c *JoinedDiffIterator) SeekGE(id Key) {
	c.p = nil
	c.iterA.SeekGE(id)
	c.iterB.SeekGE(id)
}
