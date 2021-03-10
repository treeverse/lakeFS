package committed

import (
	"bytes"
	"context"

	"github.com/treeverse/lakefs/graveler"
)

type compareIterator struct {
	ctx             context.Context
	errorOnConflict bool
	diffIt          graveler.DiffIterator
	val             *graveler.Diff
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
func NewMergeIterator(ctx context.Context, diffDestToSource graveler.DiffIterator, base Iterator) *compareValueIterator {
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
func NewCompareIterator(ctx context.Context, diffDestToSource graveler.DiffIterator, base Iterator) *compareIterator {
	return &compareIterator{
		ctx:             ctx,
		diffIt:          diffDestToSource,
		base:            base,
		errorOnConflict: false,
	}
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
	d.val = d.diffIt.Value().Copy()
	d.val.Type = graveler.DiffTypeConflict
	return true
}

func (d *compareIterator) Next() bool {
	for {
		// get next value or return with context/iterator error (if any)
		select {
		case <-d.ctx.Done():
			d.err = d.ctx.Err()
			return false
		default:
			if !d.diffIt.Next() {
				d.err = d.diffIt.Err()
				return false
			}
		}
		val := d.diffIt.Value()
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
				d.val = d.diffIt.Value().Copy()
				return true
			}
			if !bytes.Equal(baseVal.Identity, val.Value.Identity) {
				// removed on dest, but changed on source
				return d.handleConflict()
			}
			continue
		case graveler.DiffTypeChanged:
			if baseVal == nil {
				// added on dest and source, with different identities
				return d.handleConflict()
			}
			if bytes.Equal(baseVal.Identity, val.Value.Identity) {
				// changed on dest, but not on source
				continue
			}
			if !bytes.Equal(baseVal.Identity, val.LeftIdentity) {
				// changed on dest and source, to different identities
				return d.handleConflict()
			}
			// changed only on source
			d.val = d.diffIt.Value().Copy()
			return true
		case graveler.DiffTypeRemoved:
			// exists on dest, but not on source
			if baseVal != nil {
				if bytes.Equal(baseVal.Identity, val.LeftIdentity) {
					// removed on source, not changed on dest
					d.val = d.diffIt.Value().Copy()
					return true
				}
				// changed on dest, removed on source
				return d.handleConflict()
			}
			// added on dest, but not on source - continue
		}
	}
}

func (d *compareIterator) SeekGE(id graveler.Key) {
	d.val = nil
	d.err = nil
	d.diffIt.SeekGE(id)
}

func (d *compareIterator) Value() *graveler.Diff {
	if d.err != nil {
		return nil
	}
	return d.val
}

func (d *compareIterator) Err() error {
	return d.err
}

func (d *compareIterator) Close() {
	d.diffIt.Close()
	d.base.Close()
}

func (c *compareValueIterator) Value() *graveler.ValueRecord {
	value := c.compareIterator.Value()
	if value == nil {
		return nil
	}
	res := &graveler.ValueRecord{
		Key: value.Key,
	}
	if value.Type != graveler.DiffTypeRemoved {
		res.Value = value.Value
	}
	return res
}
