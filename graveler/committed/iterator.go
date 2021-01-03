package committed

import (
	"fmt"

	"github.com/treeverse/lakefs/graveler"
)

type iterator struct {
	started   bool
	manager   RangeManager
	rangesIt  graveler.ValueIterator
	it        graveler.ValueIterator
	err       error
	namespace Namespace
}

func NewIterator(manager RangeManager, namespace Namespace, rangesIt graveler.ValueIterator) Iterator {
	return &iterator{
		manager:   manager,
		namespace: namespace,
		rangesIt:  rangesIt,
	}
}

func (rvi *iterator) NextRange() bool {
	if rvi.it != nil {
		rvi.it.Close()
		rvi.it = nil
	}
	return rvi.rangesIt.Next()
}

func (rvi *iterator) Next() bool {
	if !rvi.started {
		rvi.started = true
		return rvi.NextRange()
	}
	if rvi.it != nil {
		if rvi.it.Next() {
			return true
		}
		// At end of range
		return rvi.NextRange()
	}
	// Start iterating inside the range of rvi.RangesIt
	var err error
	rngVal := rvi.rangesIt.Value()
	if rngVal == nil {
		return rvi.NextRange()
	}
	rvi.it, err = rvi.newRangeIterator(ID(rngVal.Identity))
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
	rngVal := rvi.rangesIt.Value()
	if rngVal == nil || rngVal.Value == nil {
		return nil, nil
	}
	rng, err := UnmarshalRange(rngVal.Value.Data)
	if err != nil {
		rvi.err = fmt.Errorf("unmarshal %s: %w", rngVal.Identity, err)
		return nil, nil
	}
	rng.ID = ID(rngVal.Identity)
	if rvi.it == nil {
		return nil, &rng // start new range
	}
	return rvi.it.Value(), &rng
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
	rvi.rangesIt.Close()
}

func (rvi *iterator) SeekGE(id graveler.Key) {
	var err error
	rvi.rangesIt.SeekGE(id)
	if err = rvi.rangesIt.Err(); err != nil {
		rvi.err = err
		return
	}
	rngVal := rvi.rangesIt.Value()
	if rngVal == nil {
		rvi.err = fmt.Errorf("no metarange: %w", graveler.ErrNotFound)
		return
	}
	rvi.it, err = rvi.newRangeIterator(ID(rngVal.Identity))
	if err != nil {
		rvi.err = err
		return
	}
	rvi.it.SeekGE(id)
	rvi.err = rvi.it.Err()
}

func (rvi *iterator) newRangeIterator(rangeID ID) (graveler.ValueIterator, error) {
	it, err := rvi.manager.NewRangeIterator(rvi.namespace, rangeID)
	if err != nil {
		return nil, err
	}
	return NewUnmarshalIterator(it), nil
}
