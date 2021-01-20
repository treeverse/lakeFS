package committed

import (
	"bytes"

	"github.com/treeverse/lakefs/graveler"
)

type compareIterator struct {
	errorOnConflict bool
	diffIt          graveler.DiffIterator
	val             *graveler.Diff
	base            Iterator
	err             error
}

type compareValueIterator struct {
	*compareIterator
	val *graveler.ValueRecord
}

// NewMergeIterator accepts an iterator describing a diff from theirs to ours.
// It returns a ValueIterator with the changes to perform on theirs, in order to merge ours into it,
// relative to base as the merge base.
// The iterator will return ErrConflictFound when it reaches a conflict.
func NewMergeIterator(diffTheirsToOurs graveler.DiffIterator, base Iterator) *compareValueIterator {
	return &compareValueIterator{compareIterator: &compareIterator{diffIt: diffTheirsToOurs, base: base, errorOnConflict: true}}
}

func NewCompareIterator(diffTheirsToOurs graveler.DiffIterator, base Iterator) *compareIterator {
	return &compareIterator{diffIt: diffTheirsToOurs, base: base, errorOnConflict: false}
}

func (d *compareIterator) valueFromBase(key graveler.Key) *graveler.ValueRecord {
	d.base.SeekGE(key)
	var val *graveler.ValueRecord
	for d.base.Next() && val == nil {
		val, _ = d.base.Value()
	}
	if val == nil || !bytes.Equal(val.Key, key) {
		return nil
	}
	return val
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
	for d.diffIt.Next() {
		val := d.diffIt.Value()
		key := val.Key
		typ := val.Type
		baseVal := d.valueFromBase(key)
		switch typ {
		case graveler.DiffTypeAdded:
			// exists on ours, but not on theirs
			if baseVal == nil {
				// added only on ours
				d.val = d.diffIt.Value().Copy()
				return true
			}
			if !bytes.Equal(baseVal.Identity, val.Value.Identity) {
				// removed on theirs, but changed on ours
				return d.handleConflict()
			}
			continue
		case graveler.DiffTypeChanged:
			if baseVal == nil {
				// added on theirs and ours, with different identities
				return d.handleConflict()
			}
			if bytes.Equal(baseVal.Identity, val.Value.Identity) {
				// changed on theirs, but not on ours
				continue
			}
			if !bytes.Equal(baseVal.Identity, val.LeftIdentity) {
				// changed on theirs and ours, to different identities
				return d.handleConflict()
			}
			// changed only on ours
			d.val = d.diffIt.Value().Copy()
			return true
		case graveler.DiffTypeRemoved:
			// exists on theirs, but not on ours
			if baseVal != nil {
				if bytes.Equal(baseVal.Identity, val.LeftIdentity) {
					// removed on ours, not changed on theirs
					d.val = d.diffIt.Value().Copy()
					return true
				}
				// changed on theirs, removed on ours
				return d.handleConflict()
			}
			// added on theirs, but not on ours - continue
		}
	}
	return false
}

func (d *compareIterator) SeekGE(id graveler.Key) {
	d.val = nil
	d.err = nil
	d.diffIt.SeekGE(id)
}

func (d *compareIterator) Value() *graveler.Diff {
	return d.val
}

func (d *compareIterator) Err() error {
	return d.err
}

func (d *compareIterator) Close() {
	d.diffIt.Close()
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
