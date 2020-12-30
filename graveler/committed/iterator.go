package committed

import (
	"github.com/treeverse/lakefs/graveler"
)

type iterator struct {
	started   bool
	manager   RangeManager
	ranges    []Range
	it        graveler.ValueIterator
	err       error
	namespace Namespace
}

func NewIterator(manager RangeManager, namespace Namespace, ranges []Range) Iterator {
	return &iterator{
		manager:   manager,
		namespace: namespace,
		ranges:    ranges,
	}
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
	pvi.it, err = pvi.newRangeIterator(pvi.ranges[0].ID)
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

func (pvi *iterator) newRangeIterator(rangeID ID) (graveler.ValueIterator, error) {
	it, err := pvi.manager.NewRangeIterator(pvi.namespace, rangeID, nil)
	if err != nil {
		return nil, err
	}
	return NewRangeIterator(it), nil
}
