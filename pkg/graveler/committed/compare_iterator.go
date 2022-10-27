package committed

import (
	"bytes"
	"context"
	"errors"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type compareIterator struct {
	ctx             context.Context
	errorOnConflict bool
	diffIt          DiffIterator
	val             *graveler.Diff
	rng             *RangeDiff
	base            Iterator
	err             error
}

var ErrUnsupportedRangeDiffType = errors.New("range diff type unsupported - supports only added and removed")

// NewCompareIterator accepts an iterator describing a diff from the merge destination to the source.
// It returns a DiffIterator with the changes to perform on the destination branch, in order to merge the source into it,
// relative to base as the merge base.
// When reaching a conflict, the returned Diff will be of type graveler.DiffTypeConflict.
func NewCompareIterator(ctx context.Context, diffDestToSource DiffIterator, base Iterator) *compareIterator {
	return &compareIterator{
		ctx:             ctx,
		diffIt:          diffDestToSource,
		base:            base,
		errorOnConflict: false,
	}
}

// baseGE returns value ( and its range) from base iterator, which is greater or equal than the given key
func (d *compareIterator) baseGE(key graveler.Key) (*graveler.ValueRecord, *Range, error) {
	d.base.SeekGE(key)
	if !d.base.Next() {
		return nil, nil, d.err
	}
	baseValue, baseRange := d.base.Value()
	return baseValue, baseRange, nil
}

func (d *compareIterator) valueFromBase(key graveler.Key) (*graveler.ValueRecord, error) {
	d.base.SeekGE(key)
	var val *graveler.ValueRecord
	for d.base.Next() && val == nil {
		val, _ = d.base.Value()
	}
	if err := d.base.Err(); err != nil {
		return nil, err
	}
	if val == nil || !bytes.Equal(val.Key, key) {
		return nil, nil
	}
	return val, nil
}

func (d *compareIterator) handleConflict() bool {
	if d.errorOnConflict {
		d.err = graveler.ErrConflictFound
		return false
	}
	val, rng := d.diffIt.Value()
	if val != nil {
		d.val = val.Copy()
	}
	d.setRangeDiff(rng)
	d.val.Type = graveler.DiffTypeConflict
	return true
}

func (d *compareIterator) setRangeDiff(r *RangeDiff) {
	if r != nil {
		d.rng = r.Copy()
		return
	}
	d.rng = nil
}

// stepNext is called after diffIt Next or NextRange and iterates over diff iterator until the compare iterator has a value
func (d *compareIterator) stepNext() bool {
	for {
		select {
		case <-d.ctx.Done():
			d.err = d.ctx.Err()
			return false
		default:
		}
		var hasNext bool
		var done bool
		val, _ := d.diffIt.Value()
		if val == nil {
			// range header
			hasNext, done = d.stepRange()
		} else {
			hasNext, done = d.stepValue()
		}
		if done {
			return hasNext
		}
		if !hasNext {
			break
		}
	}
	if d.err != nil {
		d.err = d.diffIt.Err()
	}
	return false
}

// stepValue moves one step according to current value
// returns hasMore if iterator has more, and done if the step is over  (got to a value, end of iterator, or error)
func (d *compareIterator) stepValue() (hasNext, done bool) {
	val, rngDiff := d.diffIt.Value()
	key := val.Key
	typ := val.Type
	baseVal, err := d.valueFromBase(key)
	if err != nil {
		d.err = err
		return false, true
	}
	switch typ {
	case graveler.DiffTypeAdded:
		// exists on source, but not on dest
		if baseVal == nil {
			// added only on source
			d.val = val.Copy()
			d.setRangeDiff(rngDiff)
			return true, true
		}
		if !bytes.Equal(baseVal.Identity, val.Value.Identity) {
			// removed on dest, but changed on source
			return d.handleConflict(), true
		}
	case graveler.DiffTypeChanged:
		if baseVal == nil {
			// added on dest and source, with different identities
			return d.handleConflict(), true
		}
		if bytes.Equal(baseVal.Identity, val.Value.Identity) {
			// changed on dest, but not on source
			return d.diffIt.Next(), false
		}
		if !bytes.Equal(baseVal.Identity, val.LeftIdentity) {
			// changed on dest and source, to different identities
			return d.handleConflict(), true
		}
		// changed only on source
		d.val = val.Copy()
		d.setRangeDiff(rngDiff)
		return true, true
	case graveler.DiffTypeRemoved:
		// exists on dest, but not on source
		if baseVal != nil {
			if bytes.Equal(baseVal.Identity, val.LeftIdentity) {
				// removed on source, not changed on dest
				d.val = val.Copy()
				d.setRangeDiff(rngDiff)
				return true, true
			}
			// changed on dest, removed on source
			return d.handleConflict(), true
		}
		// added on dest, but not on source - next value
	}
	return d.diffIt.Next(), false
}

// stepRange moves one step according to current range
// returns hasMore if iterator has more, and done if the step is over (got to a value, end of iterator, or error)
func (d *compareIterator) stepRange() (hasMore bool, done bool) {
	_, rngDiff := d.diffIt.Value()
	typ := rngDiff.Type
	rng := rngDiff.Range
	leftID := rngDiff.LeftIdentity
	baseValue, baseRange, err := d.baseGE(graveler.Key(rng.MinKey))
	if err != nil {
		d.err = err
		return false, true
	}
	switch typ {
	case graveler.DiffTypeAdded:
		// exists on source, but not on dest
		if baseRange != nil && rng.ID == baseRange.ID {
			// removed only on dest -> skip range
			return d.diffIt.NextRange(), false
		}
		if baseValue == nil || bytes.Compare(rng.MaxKey, baseValue.Key) < 0 {
			// added only on source
			d.setRangeDiff(rngDiff)
			d.val = nil
			return true, true
		}
		// overlapping base and diff, must step into range
		return d.diffIt.Next(), false
	case graveler.DiffTypeRemoved:
		if baseRange != nil && rng.ID == baseRange.ID {
			// removed on source, not changed on dest
			d.setRangeDiff(rngDiff)
			d.val = nil
			return true, true
		}
		if (baseRange != nil && bytes.Compare(rng.MaxKey, baseRange.MinKey) < 0) || baseValue == nil || bytes.Compare(rng.MaxKey, baseValue.Key) < 0 {
			// added on dest, but not on source, skip range
			return d.diffIt.NextRange(), false
		}

		// overlapping base and diff, must step into range
		return d.diffIt.Next(), false

	case graveler.DiffTypeChanged:
		if baseRange != nil && leftID == baseRange.ID {
			// changed only in source
			d.setRangeDiff(rngDiff)
			d.val = nil
			return true, true
		}
		if baseRange != nil && bytes.Compare(rng.MaxKey, baseRange.MinKey) < 0 {
			// conflict, added on dest and source
			return d.handleConflict(), true
		}
		if baseRange != nil && rng.ID == baseRange.ID {
			// changed on dest, but not on source, skip range
			return d.diffIt.NextRange(), false
		}
		return d.diffIt.Next(), false
	default:
		d.err = ErrUnsupportedRangeDiffType
	}
	return false, true
}

func (d *compareIterator) Next() bool {
	if d.diffIt.Next() {
		return d.stepNext()
	}
	d.err = d.diffIt.Err()
	return false
}

func (d *compareIterator) NextRange() bool {
	if !d.diffIt.NextRange() {
		d.err = d.diffIt.Err()
		return false
	}
	return d.stepNext()
}

func (d *compareIterator) SeekGE(id graveler.Key) {
	d.val = nil
	d.err = nil
	d.diffIt.SeekGE(id)
}

func (d *compareIterator) Value() (*graveler.Diff, *RangeDiff) {
	if d.err != nil {
		return nil, nil
	}
	return d.val, d.rng
}

func (d *compareIterator) Err() error {
	return d.err
}

func (d *compareIterator) Close() {
	d.diffIt.Close()
	d.base.Close()
}
