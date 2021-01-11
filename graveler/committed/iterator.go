package committed

import (
	"fmt"

	"github.com/treeverse/lakefs/graveler"
)

type iterator struct {
	started   bool
	manager   RangeManager
	rangesIt  ValueIterator
	rng       *Range // Decoded value at which rangeIt point
	it        graveler.ValueIterator
	err       error
	namespace Namespace
}

func NewIterator(manager RangeManager, namespace Namespace, rangesIt ValueIterator) Iterator {
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
	rvi.rng = nil
	if !rvi.rangesIt.Next() {
		return false
	}
	rngRecord := rvi.rangesIt.Value()
	if rngRecord == nil {
		return rvi.NextRange()
	}

	rng, err := UnmarshalRange(rngRecord.Value)
	if err != nil {
		rvi.err = fmt.Errorf("unmarshal %s: %w", string(rngRecord.Key), err)
		return false
	}

	rng.ID = ID(rngRecord.Key)
	rvi.rng = &rng

	it, err := rvi.manager.NewRangeIterator(rvi.namespace, rvi.rng.ID)
	if err != nil {
		rvi.err = fmt.Errorf("open range %s: %w", rvi.rng.ID, err)
		return false
	}
	rvi.it = NewUnmarshalIterator(it)

	return true
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
	if rvi.rng == nil {
		return rvi.NextRange()
	}
	if rvi.it.Next() {
		return true
	}
	// Already at end of empty range
	return rvi.NextRange()
}

func (rvi *iterator) Value() (*graveler.ValueRecord, *Range) {
	if rvi.it == nil {
		return nil, rvi.rng // start new range
	}
	return rvi.it.Value(), rvi.rng
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

func (rvi *iterator) SeekGE(key graveler.Key) {
	var err error
	// TODO(ariels): rangesIt might already be on correct range.
	rvi.rangesIt.SeekGE(Key(key))
	if err = rvi.rangesIt.Err(); err != nil {
		rvi.err = err
		return
	}
	if !rvi.NextRange() {
		return // Reached end.
	}
	rvi.started = true // "Started": rangesIt is valid.
	rvi.it.SeekGE(key)
	// Ready to call Next to see values.
	rvi.err = rvi.it.Err()
}

type emptyIterator struct{}

func NewEmptyIterator() Iterator {
	return &emptyIterator{}
}

func (e *emptyIterator) Next() bool {
	return false
}

func (e *emptyIterator) NextRange() bool {
	return false
}

func (e *emptyIterator) Value() (*graveler.ValueRecord, *Range) {
	return nil, nil
}

func (e *emptyIterator) SeekGE(graveler.Key) {}

func (e *emptyIterator) Err() error {
	return nil
}

func (e *emptyIterator) Close() {}
