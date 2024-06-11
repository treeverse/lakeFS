package graveler

import (
	"bytes"
	"errors"

	"github.com/treeverse/lakefs/pkg/logging"
)

// JoinedDiffIterator calculate the union diff between 2 iterators.
// The output iterator yields a single result for each key.
// If a key exist in the 2 iterators, iteratorA value prevails.
type JoinedDiffIterator struct {
	iterA        DiffIterator
	iterAHasMore bool
	iterB        DiffIterator
	iterBHasMore bool
	// the current iterator that has the value to return (iterA or iterB)
	currentIter DiffIterator
	started     bool
	log         logging.Logger
}

func NewJoinedDiffIterator(iterA DiffIterator, iterB DiffIterator) *JoinedDiffIterator {
	return &JoinedDiffIterator{
		iterA:        iterA,
		iterAHasMore: true,
		iterB:        iterB,
		iterBHasMore: true,
		currentIter:  nil,
	}
}

// progressIterByKey advances the iterators to the next key when both iterators still has more keys
func (c *JoinedDiffIterator) progressIterByKey(keyA Key, keyB Key) {
	compareResult := bytes.Compare(keyA, keyB)
	switch {
	case compareResult == 0:
		// key exists in both iterators
		c.iterAHasMore = c.iterA.Next()
		c.iterBHasMore = c.iterB.Next()
	case compareResult < 0:
		// value of iterA > value of iterB
		c.iterAHasMore = c.iterA.Next()
	default:
		// value of iterA < value of iterB
		c.iterBHasMore = c.iterB.Next()
	}
}

func (c *JoinedDiffIterator) Next() bool {
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
		c.currentIter = c.iterB
		c.iterBHasMore = c.iterB.Next()
	case !c.iterBHasMore:
		// iterB is done
		c.currentIter = c.iterA
		c.iterAHasMore = c.iterA.Next()
	default:
		// both iterators has more keys- progress by key
		c.progressIterByKey(c.iterA.Value().Key, c.iterB.Value().Key)
	}
	if c.iterA.Err() != nil {
		c.currentIter = c.iterA
		c.iterAHasMore = false
		c.iterBHasMore = false
		return false
	}
	if c.iterB.Err() != nil {
		c.currentIter = c.iterB
		c.iterAHasMore = false
		c.iterBHasMore = false
		return false
	}

	// set c.currentIter to be the next (smaller) value

	// if one of the iterators is done, set the other one as the current iterator and return
	if !c.iterAHasMore {
		c.currentIter = c.iterB
		return c.iterBHasMore
	} else if !c.iterBHasMore {
		c.currentIter = c.iterA
		return true
	}

	// if both iterators has more keys, set the iterator with the smaller key as the current iterator
	if bytes.Compare(c.iterA.Value().Key, c.iterB.Value().Key) <= 0 {
		c.currentIter = c.iterA
	} else {
		c.currentIter = c.iterB
	}
	return true
}

func (c *JoinedDiffIterator) Value() *Diff {
	if c.currentIter == nil {
		c.log.Errorf("current iterator is nil")
		return nil
	}
	return c.currentIter.Value()
}

func (c *JoinedDiffIterator) Err() error {
	if c.iterA.Err() != nil && c.iterB.Err() != nil {
		return errors.Join(c.iterA.Err(), c.iterB.Err())
	}
	if c.iterA.Err() != nil {
		return c.iterA.Err()
	}
	if c.iterB.Err() != nil {
		return c.iterB.Err()
	}
	return nil
}

func (c *JoinedDiffIterator) Close() {
	c.iterA.Close()
	c.iterB.Close()
}

func (c *JoinedDiffIterator) SeekGE(id Key) {
	c.currentIter = nil
	c.iterA.SeekGE(id)
	c.iterB.SeekGE(id)
}
