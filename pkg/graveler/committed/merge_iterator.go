package committed

import (
	"bytes"
	"context"

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

type compareValueIterator struct {
	*compareIterator
}

// NewMergeIterator accepts an iterator describing a diff from the merge destination to the source.
// It returns a graveler.ValueIterator with the changes to perform on the destination branch, in order to merge the source into it,
// relative to base as the merge base.
// When reaching a conflict, the iterator will enter an error state with the graveler.ErrConflictFound error.
func NewMergeIterator(ctx context.Context, diffDestToSource DiffIterator, base Iterator) *compareValueIterator {
	return &compareValueIterator{
		compareIterator: &compareIterator{
			ctx:             ctx,
			diffIt:          diffDestToSource,
			base:            base,
			errorOnConflict: true,
		},
	}
}

// NewCompareIterator accepts an iterator describing a diff from the merge destination to the source.
// It returns a graveler.DiffIterator with the changes to perform on the destination branch, in order to merge the source into it,
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

func (d *compareIterator) rangeFromBase(key graveler.Key) (*Range, error) {
	d.base.SeekGE(key)
	if !d.base.Next() {
		if d.err != nil {
			return nil, d.err
		}
		return nil, nil
	}
	_, baseRange := d.base.Value()
	return baseRange, nil
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
	val, _ := d.diffIt.Value()
	d.val = val.Copy()
	d.val.Type = graveler.DiffTypeConflict
	return true
}

func (d *compareIterator) Step() bool {
	hasNext := true
	for hasNext {
		select {
		case <-d.ctx.Done():
			d.err = d.ctx.Err()
			return false
		default:
		}
		val, rngDiff := d.diffIt.Value()
		if rngDiff != nil {
			typ := rngDiff.Type
			rng := rngDiff.Range
			baseRange, err := d.rangeFromBase(graveler.Key(rng.MinKey))
			if err != nil {
				d.err = err
				return false
			}
			switch typ {
			case graveler.DiffTypeAdded:
				// exists on source, but not on dest
				if baseRange == nil || bytes.Compare(rng.MaxKey, baseRange.MinKey) < 0 { // TODO(Guys): change this - maybe change d.range from base that it will return
					// added only on source
					d.rng = rngDiff
					d.val = nil
					return true
				}
				if baseRange.ID != rng.ID {
					// removed on dest, but changed on source
					hasNext = d.diffIt.Next()
				}
				d.diffIt.Next()
			case graveler.DiffTypeRemoved:
				// exists on dest, but not on source
				if baseRange != nil {
					if baseRange.ID == rng.ID {
						// removed on source, not changed on dest
						d.rng = rngDiff
						d.val = nil
						return true
					}
					// changed on dest, removed on source
					d.diffIt.Next() // go in
				}
				// added on dest, but not on source - continue -> nextRange
				hasNext = d.diffIt.Next()
			}
		}
		if val != nil {
			key := val.Key
			typ := val.Type
			baseVal, err := d.valueFromBase(key)
			if err != nil {
				d.err = err
				return false
			}
			switch typ {
			case graveler.DiffTypeAdded:
				// exists on source, but not on dest
				if baseVal == nil {
					// added only on source
					d.val = val.Copy()
					d.rng = nil
					return true
				}
				if !bytes.Equal(baseVal.Identity, val.Value.Identity) {
					// removed on dest, but changed on source
					return d.handleConflict()
				}
			case graveler.DiffTypeChanged:
				if baseVal == nil {
					// added on dest and source, with different identities
					return d.handleConflict()
				}
				if bytes.Equal(baseVal.Identity, val.Value.Identity) {
					// changed on dest, but not on source
					hasNext = d.diffIt.Next()
					continue
				}
				if !bytes.Equal(baseVal.Identity, val.LeftIdentity) {
					// changed on dest and source, to different identities
					return d.handleConflict()
				}
				// changed only on source
				d.val = val.Copy()
				d.rng = nil
				return true
			case graveler.DiffTypeRemoved:
				// exists on dest, but not on source
				if baseVal != nil {
					if bytes.Equal(baseVal.Identity, val.LeftIdentity) {
						// removed on source, not changed on dest
						d.val = val.Copy()
						d.rng = nil
						return true
					}
					// changed on dest, removed on source
					return d.handleConflict()
				}
				// added on dest, but not on source - next value
			}
			hasNext = d.diffIt.Next()
		}
	}
	if d.err != nil {
		d.err = d.diffIt.Err()
	}
	return false
}

func (d *compareIterator) Next() bool {
	if d.diffIt.Next() {
		return d.Step()
	}

	d.err = d.diffIt.Err()
	return false
}

func (d *compareIterator) NextRange() bool {
	if !d.diffIt.NextRange() {
		d.err = d.diffIt.Err()
		return false
	}
	return d.Step()
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

func (c *compareValueIterator) Value() (*graveler.ValueRecord, *Range) {
	value, rng := c.compareIterator.Value()
	if value != nil {
		res := &graveler.ValueRecord{
			Key: value.Key,
		}
		if value.Type != graveler.DiffTypeRemoved {
			res.Value = value.Value
		}
		return res, nil
	}
	if rng != nil {
		res := rng.Range.Copy()
		if rng.Type == graveler.DiffTypeRemoved {
			res.SetTombstone()
		}
		return nil, res
	}
	return nil, nil
}
