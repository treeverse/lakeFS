package committed

import "github.com/treeverse/lakefs/graveler"

type iterator struct {
	started          bool
	metaRangeManager MetaRangeManager
	ranges           []Range
	it               graveler.ValueIterator
	err              error
}

func NewIterator(repo MetaRangeManager, ranges []Range) Iterator {
	return &iterator{metaRangeManager: repo, ranges: ranges}
}

func (pvi *iterator) NextRange() bool {
	if len(pvi.ranges) <= 1 {
		return false
	}
	pvi.ranges = pvi.ranges[1:]
	pvi.it.Close()
	pvi.it = nil
	return true
}

func (pvi *iterator) Next() bool {
	if !pvi.started {
		pvi.started = true
		return len(pvi.ranges) > 0
	}
	if pvi.it != nil {
		if pvi.it.Next() {
			return true
		}
		// At end of range
		return pvi.NextRange()
	}
	// Start iterating inside range
	if len(pvi.ranges) == 0 {
		return false // Iteration was already finished.
	}
	var err error
	pvi.it, err = pvi.metaRangeManager.NewRangeIterator(pvi.ranges[0].ID, nil)
	if err != nil {
		pvi.err = err
		return false
	}
	if pvi.it.Next() {
		return true
	}
	// Already at end of empty range
	return pvi.NextRange()
}

func (pvi *iterator) Value() (*graveler.ValueRecord, *Range) {
	if len(pvi.ranges) == 0 {
		return nil, nil
	}
	rng := &pvi.ranges[0]
	if pvi.it == nil {
		return nil, rng // start new range
	}
	return pvi.it.Value(), rng
}

func (pvi *iterator) Err() error {
	if pvi.err != nil {
		return pvi.err
	}
	if pvi.it == nil {
		return nil
	}
	return pvi.it.Err()
}

func (pvi *iterator) Close() {
	if pvi.it == nil {
		return
	}
	pvi.it.Close()
}
