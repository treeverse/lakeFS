package graveler

import "bytes"

// CombinedDiffIterator calculates the diff between a commit and a branch, including the staging area of the branch.
// committedDiffIterator is the DiffIterator between the commit and the HEAD of the branch.
// stagingIterator is the ValueIterator on the staging area of the branch
// leftIterator is the ValueIterator on the commit
type CombinedDiffIterator struct {
	started               bool
	committedDiffIterator DiffIterator
	leftIterator          ValueIterator
	stagingIterator       ValueIterator

	committedDiff *Diff
	stagingValue  *ValueRecord

	val *Diff
	err error
}

func NewCombinedDiffIterator(committedDiffIterator DiffIterator, leftIterator ValueIterator, stagingIterator ValueIterator) *CombinedDiffIterator {
	return &CombinedDiffIterator{committedDiffIterator: committedDiffIterator, leftIterator: leftIterator, stagingIterator: stagingIterator}
}

func (c *CombinedDiffIterator) loadNextStagingValue() {
	c.stagingValue = nil
	if c.stagingIterator.Next() {
		c.stagingValue = c.stagingIterator.Value()
	} else if c.stagingIterator.Err() != nil {
		c.err = c.stagingIterator.Err()
	}
}

func (c *CombinedDiffIterator) loadNextCommittedDiff() {
	c.committedDiff = nil
	if c.committedDiffIterator.Next() {
		c.committedDiff = c.committedDiffIterator.Value()
	} else if c.committedDiffIterator.Err() != nil {
		c.err = c.committedDiffIterator.Err()
	}
}

func (c *CombinedDiffIterator) Next() bool {
	if !c.started {
		c.started = true
		c.loadNextStagingValue()
		c.loadNextCommittedDiff()
	}
	for c.committedDiff != nil || c.stagingValue != nil {
		if c.err != nil {
			return false
		}
		if c.stagingValue == nil {
			// nothing on staging - return the original diff
			c.val = c.committedDiff
			c.loadNextCommittedDiff()
			return true
		}
		committedStagingCompareResult := 1 // for the case where committedDiff == nil
		if c.committedDiff != nil {
			committedStagingCompareResult = bytes.Compare(c.committedDiff.Key, c.stagingValue.Key)
		}
		if committedStagingCompareResult < 0 {
			// nothing on staging - return the original diff
			c.val = c.committedDiff
			c.loadNextCommittedDiff()
			return true
		}
		// something on staging
		compareStagingResult := c.compareStagingWithLeft()
		c.loadNextStagingValue()
		if committedStagingCompareResult == 0 {
			// both sides had values - need to advance committed iterator
			c.loadNextCommittedDiff()
		}
		if compareStagingResult {
			return true
		}
	}
	return false
}

// compareStagingWithLeft checks if there is a diff between the left side to the staging area.
// For the last key fetched from the staging iterator, it checks the left-side iterator for the value of the same key.
// If there is a diff between them, it sets the diff and returns true. Otherwise, false is returned.
func (c *CombinedDiffIterator) compareStagingWithLeft() bool {
	c.leftIterator.SeekGE(c.stagingValue.Key)
	var leftVal *ValueRecord
	if c.leftIterator.Next() {
		leftVal = c.leftIterator.Value()
		if !bytes.Equal(leftVal.Key, c.stagingValue.Key) {
			// key wasn't on left side
			leftVal = nil
		}
	} else if c.leftIterator.Err() != nil {
		c.err = c.leftIterator.Err()
		return false
	}
	var typ DiffType
	var leftIdentity []byte
	value := c.stagingValue.Value
	switch {
	case leftVal == nil:
		if c.stagingValue.IsTombstone() {
			// not on left, deleted on staging - no diff
			return false
		}
		// not on left, but is on staging
		typ = DiffTypeAdded
	case c.stagingValue.IsTombstone():
		// found on left, deleted on staging
		leftIdentity = leftVal.Identity
		value = leftVal.Value
		typ = DiffTypeRemoved
	default:
		// found on both sides
		leftIdentity = leftVal.Identity
		if bytes.Equal(c.stagingValue.Identity, leftVal.Identity) {
			// identity not changed - no diff
			return false
		}
		typ = DiffTypeChanged
	}
	c.val = &Diff{
		Type:         typ,
		Key:          c.stagingValue.Key,
		Value:        value,
		LeftIdentity: leftIdentity,
	}
	return true
}

func (c *CombinedDiffIterator) SeekGE(id Key) {
	c.started = false
	c.committedDiffIterator.SeekGE(id)
	c.stagingIterator.SeekGE(id)
}

func (c *CombinedDiffIterator) Value() *Diff {
	return c.val
}

func (c *CombinedDiffIterator) Err() error {
	return c.err
}

func (c *CombinedDiffIterator) Close() {
	c.committedDiffIterator.Close()
	c.leftIterator.Close()
	c.stagingIterator.Close()
}
