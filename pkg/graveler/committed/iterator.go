package committed

import (
	"bytes"
	"context"
	"fmt"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type iterator struct {
	ctx         context.Context
	started     bool
	manager     RangeManager
	rangesIt    ValueIterator
	rng         *Range                 // Decoded value at which rangeIt point
	it          graveler.ValueIterator // nil at start of range
	err         error
	namespace   Namespace
	beforeRange bool
}

func NewIterator(ctx context.Context, manager RangeManager, namespace Namespace, rangesIt ValueIterator) Iterator {
	return &iterator{
		ctx:       ctx,
		manager:   manager,
		namespace: namespace,
		rangesIt:  rangesIt,
	}
}

// loadIt loads rvi.it to start iterating over a new range.  It returns false and sets rvi.err
// if it fails to open the new range.
func (rvi *iterator) loadIt() bool {
	it, err := rvi.manager.NewRangeIterator(rvi.ctx, rvi.namespace, rvi.rng.ID)
	if err != nil {
		rvi.err = fmt.Errorf("open range %s: %w", rvi.rng.ID, err)
		return false
	}
	rvi.it = NewUnmarshalIterator(it)
	return true
}

func (rvi *iterator) NextRange() bool {
	if rvi.it != nil {
		rvi.it.Close()
	}
	rvi.it = nil
	rvi.rng = nil

	var rngRecord *Record
	for rngRecord == nil { // Skip this and any consecutive finished ranges.
		if !rvi.rangesIt.Next() {
			return false
		}
		rngRecord = rvi.rangesIt.Value()
	}

	gv, err := UnmarshalValue(rngRecord.Value, "", "", graveler.Key(""))
	if err != nil {
		rvi.err = fmt.Errorf("unmarshal value for %s: %w", string(rngRecord.Key), err)
		return false
	}

	rng, err := UnmarshalRange(gv.Data)
	if err != nil {
		rvi.err = fmt.Errorf("unmarshal %s: %w", string(rngRecord.Key), err)
		return false
	}

	rng.ID = ID(gv.Identity)
	rvi.rng = &rng

	return true
}

func (rvi *iterator) Next() bool {
	if rvi.err != nil {
		return false
	}
	if rvi.beforeRange {
		rvi.beforeRange = false
		return true
	}
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

	if !rvi.loadIt() {
		return false
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
	rvi.rangesIt.Close()
	if rvi.it == nil {
		return
	}
	rvi.it.Close()
}

func (rvi *iterator) loadRange(key graveler.Key) bool {
	rvi.rangesIt.SeekGE(Key(key))
	if err := rvi.rangesIt.Err(); err != nil {
		rvi.err = err
		return false
	}
	if !rvi.NextRange() {
		return false // Reached end
	}
	rvi.started = true // "Started": rangesIt is valid.
	if bytes.Compare(key, rvi.rng.MinKey) <= 0 {
		// the given key is before the next range
		rvi.beforeRange = true
		return false
	}
	return rvi.loadIt()
}

func (rvi *iterator) SeekGE(key graveler.Key) {
	if rvi.rng == nil || rvi.it == nil || bytes.Compare(key, rvi.rng.MinKey) < 0 || bytes.Compare(key, rvi.rng.MaxKey) > 0 {
		// no current range, or new key outside current range boundaries
		if !rvi.loadRange(key) {
			return
		}
	}
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
