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

func (rvi *iterator) NextRange() bool {
	if len(rvi.ranges) <= 1 {
		return false
	}
	rvi.ranges = rvi.ranges[1:]
	rvi.it.Close()
	rvi.it = nil
	return true
}

func (rvi *iterator) Next() bool {
	if !rvi.started {
		rvi.started = true
		return len(rvi.ranges) > 0
	}
	if rvi.it != nil {
		if rvi.it.Next() {
			return true
		}
		// At end of range
		return rvi.NextRange()
	}
	// Start iterating inside range
	if len(rvi.ranges) == 0 {
		return false // Iteration was already finished.
	}
	var err error
	rvi.it, err = rvi.metaRangeManager.NewRangeIterator(rvi.ranges[0].ID, nil)
	if err != nil {
		rvi.err = err
		return false
	}
	if rvi.it.Next() {
		return true
	}
	// Already at end of empty range
	return rvi.NextRange()
}

func (rvi *iterator) Value() (*graveler.ValueRecord, *Range) {
	if len(rvi.ranges) == 0 {
		return nil, nil
	}
	rng := &rvi.ranges[0]
	if rvi.it == nil {
		return nil, rng // start new range
	}
	return rvi.it.Value(), rng
}

func (rvi *iterator) Err() error {
	if rvi.err != nil {
		return rvi.err
	}
	if rvi.it == nil {
		return nil
	}
	return rvi.it.Err()
}

func (rvi *iterator) Close() {
	if rvi.it == nil {
		return
	}
	rvi.it.Close()
}

func (rvi *iterator) SeekGE(id graveler.Key) {
	panic("implement me")
}
