package graveler

import "bytes"

// CompactedDiffIterator calculate the diff between commit to staged + compacted area
type CompactedDiffIterator struct {
	stageAndCommittedDiffIterator     DiffIterator
	compactedAndCommittedDiffIterator DiffIterator
	p                                 DiffIterator
}

func NewCompactedDiffIterator(stageAndCommittedDiffIterator DiffIterator, compactedAndCommittedDiffIterator DiffIterator) *CompactedDiffIterator {
	return &CompactedDiffIterator{
		stageAndCommittedDiffIterator:     stageAndCommittedDiffIterator,
		compactedAndCommittedDiffIterator: compactedAndCommittedDiffIterator,
		p:                                 nil,
	}
}

// advanceInnerIterators advances the inner iterators and returns true if it has more
func (c *CompactedDiffIterator) advanceInnerIterators() bool {
	valA := c.stageAndCommittedDiffIterator.Value()
	valB := c.compactedAndCommittedDiffIterator.Value()
	var nextA bool
	var nextB bool
	switch {
	case c.p == nil:
		// first
		nextA = c.stageAndCommittedDiffIterator.Next()
		nextB = c.compactedAndCommittedDiffIterator.Next()
	case valA == nil && valB == nil:
		// last
		return false
	case valA == nil:
		// stageAndCommittedDiffIterator is done
		c.p = c.compactedAndCommittedDiffIterator
		nextB = c.compactedAndCommittedDiffIterator.Next()
	case valB == nil:
		// compactedAndCommittedDiffIterator is done
		c.p = c.stageAndCommittedDiffIterator
		nextA = c.stageAndCommittedDiffIterator.Next()
	case bytes.Equal(valA.Key, valB.Key):
		nextA = c.stageAndCommittedDiffIterator.Next()
		nextB = c.compactedAndCommittedDiffIterator.Next()
	case bytes.Compare(valA.Key, valB.Key) < 0:
		nextA = c.stageAndCommittedDiffIterator.Next()
		nextB = true
	default:
		// value of stageAndCommittedDiffIterator < value of compactedAndCommittedDiffIterator
		nextB = c.compactedAndCommittedDiffIterator.Next()
		nextA = true
	}
	if c.stageAndCommittedDiffIterator.Err() != nil {
		c.p = c.stageAndCommittedDiffIterator
		return false
	}
	if c.compactedAndCommittedDiffIterator.Err() != nil {
		c.p = c.compactedAndCommittedDiffIterator
		return false
	}
	return nextA || nextB
}

func (c *CompactedDiffIterator) Next() bool {
	if !c.advanceInnerIterators() {
		return false
	}
	// set c.p to be the next (smaller) value
	valA := c.stageAndCommittedDiffIterator.Value()
	valB := c.compactedAndCommittedDiffIterator.Value()
	switch {
	case valA == nil && valB == nil:
		c.p = c.stageAndCommittedDiffIterator
		return false
	case valA == nil:
		c.p = c.compactedAndCommittedDiffIterator
	case valB == nil:
		c.p = c.stageAndCommittedDiffIterator
	case bytes.Compare(valA.Key, valB.Key) <= 0:
		c.p = c.stageAndCommittedDiffIterator
	default:
		c.p = c.compactedAndCommittedDiffIterator
	}
	return true
}

func (c *CompactedDiffIterator) Value() *Diff {
	if c.p == nil {
		return nil
	}
	return c.p.Value()
}

func (c *CompactedDiffIterator) Err() error {
	if c.p == nil {
		return nil
	}
	return c.p.Err()
}

func (c *CompactedDiffIterator) Close() {
	c.stageAndCommittedDiffIterator.Close()
	c.compactedAndCommittedDiffIterator.Close()
}

func (c *CompactedDiffIterator) SeekGE(id Key) {
	c.p = nil
	c.stageAndCommittedDiffIterator.SeekGE(id)
	c.compactedAndCommittedDiffIterator.SeekGE(id)
}
