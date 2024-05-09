package graveler

import "bytes"

// JoinedDiffIterator calculate the union diff between 2 iterators.
// The output iterator yields a single result for each key.
// If a key exist in the 2 iterators, iteratorA value prevails.
type JoinedDiffIterator struct {
	iterA        DiffIterator
	iterAHasMore bool
	iterB        DiffIterator
	iterBHasMore bool
	p            DiffIterator
	started      bool
}

func NewJoinedDiffIterator(iterA DiffIterator, iterB DiffIterator) *JoinedDiffIterator {
	return &JoinedDiffIterator{
		iterA:        iterA,
		iterAHasMore: true,
		iterB:        iterB,
		iterBHasMore: true,
		p:            nil,
	}
}

func (c *JoinedDiffIterator) Next() bool {
	valA := c.iterA.Value()
	valB := c.iterB.Value()
	switch {
	case !c.started:
		// first
		c.iterAHasMore = c.iterA.Next()
		c.iterBHasMore = c.iterB.Next()
		c.started = true
	case !c.iterAHasMore && !c.iterBHasMore:
		// last
		return false
	case !c.iterAHasMore:
		// iterA is done
		c.p = c.iterB
		c.iterBHasMore = c.iterB.Next()
	case !c.iterBHasMore:
		// iterB is done
		c.p = c.iterA
		c.iterAHasMore = c.iterA.Next()
	case bytes.Equal(valA.Key, valB.Key):
		c.iterAHasMore = c.iterA.Next()
		c.iterBHasMore = c.iterB.Next()
	case bytes.Compare(valA.Key, valB.Key) < 0:
		c.iterAHasMore = c.iterA.Next()
		c.iterBHasMore = true
	default:
		// value of iterA < value of iterB
		c.iterBHasMore = c.iterB.Next()
		c.iterAHasMore = true
	}
	if c.iterA.Err() != nil {
		c.p = c.iterA
		c.iterAHasMore = false
		c.iterBHasMore = false
		return false
	}
	if c.iterB.Err() != nil {
		c.p = c.iterB
		c.iterAHasMore = false
		c.iterBHasMore = false
		return false
	}
	if !c.iterAHasMore && !c.iterBHasMore {
		return false
	}

	// set c.p to be the next (smaller) value
	valA = c.iterA.Value()
	valB = c.iterB.Value()
	switch {
	case !c.iterAHasMore && !c.iterBHasMore:
		c.p = c.iterA
		return false
	case !c.iterAHasMore:
		c.p = c.iterB
	case !c.iterBHasMore:
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
