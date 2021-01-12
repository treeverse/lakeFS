package committed

import (
	"bytes"

	"github.com/treeverse/lakefs/graveler"
)

type mergeIterator struct {
	diffIt graveler.DiffIterator
	val    *graveler.ValueRecord
	base   Iterator
	err    error
}

// NewMergeIterator accepts an iterator describing a diff from theirs to ours.
// It returns a ValueIterator with the changes to perform on theirs, in order to merge ours into it,
// relative to base as the merge base.
// The iterator will return ErrConflictFound when it reaches a conflict.
func NewMergeIterator(diffTheirsToOurs graveler.DiffIterator, base Iterator) (*mergeIterator, error) {
	return &mergeIterator{diffIt: diffTheirsToOurs, base: base}, nil
}

func (d *mergeIterator) valueFromBase(key graveler.Key) *graveler.ValueRecord {
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

func (d *mergeIterator) Next() bool {
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
				d.setValue()
				return true
			}
			if !bytes.Equal(baseVal.Identity, val.Value.Identity) {
				// removed on theirs, but changed on ours
				d.err = graveler.ErrConflictFound
				return false
			}
			continue
		case graveler.DiffTypeChanged:
			if baseVal == nil {
				// value was added on theirs and ours, with different identities
				d.err = graveler.ErrConflictFound
				return false
			}
			if bytes.Equal(baseVal.Identity, val.Value.Identity) {
				// value was changed on theirs, but not on ours
				continue
			}
			if !bytes.Equal(baseVal.Identity, val.LeftIdentity) {
				// value was changed on theirs and ours, to different identities
				d.err = graveler.ErrConflictFound
				return false
			}
			d.setValue()
			return true
		case graveler.DiffTypeRemoved:
			// exists on theirs, but not on ours
			if baseVal != nil {
				if bytes.Equal(baseVal.Identity, val.LeftIdentity) {
					// removed on ours, not changed on theirs
					d.setValue()
					return true
				}
				// changed on theirs, removed on ours
				d.err = graveler.ErrConflictFound
			}
			// added on theirs, but not on ours - continue
		}
	}
	return false
}

func (d *mergeIterator) setValue() {
	diff := d.diffIt.Value()
	if diff.Type == graveler.DiffTypeRemoved {
		d.val = &graveler.ValueRecord{Key: diff.Key}
	} else {
		d.val = &graveler.ValueRecord{
			Key:   diff.Key,
			Value: diff.Value,
		}
	}
}

func (d *mergeIterator) SeekGE(id graveler.Key) {
	d.val = nil
	d.err = nil
	d.diffIt.SeekGE(id)
}

func (d *mergeIterator) Value() *graveler.ValueRecord {
	return d.val
}

func (d *mergeIterator) Err() error {
	return d.err
}

func (d *mergeIterator) Close() {
	d.diffIt.Close()
}
