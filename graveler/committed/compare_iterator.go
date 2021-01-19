package committed

import (
	"bytes"

	"github.com/treeverse/lakefs/graveler"
)

type conflictCompareResult struct {
	graveler.Diff
}

type compareIterator struct {
	diffIt     graveler.DiffIterator
	base       Iterator
	isConflict bool
}

func NewCompareIterator(diffTheirsToOurs graveler.DiffIterator, base Iterator) *compareIterator {
	return &compareIterator{diffIt: diffTheirsToOurs, base: base}
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

func (d *compareIterator) Next() bool {
	d.isConflict = false
	for d.diffIt.Next() {
		val := d.diffIt.Value()
		key := val.Key().Copy()
		typ := val.Type()
		baseVal := d.valueFromBase(key)
		switch typ {
		case graveler.DiffTypeAdded:
			// exists on ours, but not on theirs
			if baseVal == nil {
				// added only on ours
				return true
			}
			if !bytes.Equal(baseVal.Identity, val.Value().Identity) {
				// removed on theirs, but changed on ours
				d.isConflict = true
				return true
			}
			continue
		case graveler.DiffTypeChanged:
			if baseVal == nil {
				// added on theirs and ours, with different identities
				d.isConflict = true
				return true
			}
			if bytes.Equal(baseVal.Identity, val.Value().Identity) {
				// changed on theirs, but not on ours
				continue
			}
			if !bytes.Equal(baseVal.Identity, val.LeftIdentity()) {
				// changed on theirs and ours, to different identities
				d.isConflict = true
				return true
			}
			// changed only on ours
			return true
		case graveler.DiffTypeRemoved:
			// exists on theirs, but not on ours
			if baseVal != nil {
				if !bytes.Equal(baseVal.Identity, val.LeftIdentity()) {
					// changed on theirs, removed on ours
					d.isConflict = true
					return true
				}
				// removed on ours, not changed on theirs
				return true
			}
			// added on theirs, but not on ours - continue
		}
	}
	return false
}

func (d *compareIterator) SeekGE(id graveler.Key) {
	d.diffIt.SeekGE(id)
	d.isConflict = false
}

func (d *compareIterator) Value() graveler.Diff {
	if !d.isConflict {
		return d.diffIt.Value()
	}
	return &conflictCompareResult{d.diffIt.Value()}
}

func (d *compareIterator) Close() {
	d.diffIt.Close()
	d.base.Close()
}

func (d *compareIterator) Err() error {
	return d.diffIt.Err()
}

func (cr *conflictCompareResult) Type() graveler.DiffType {
	return graveler.DiffTypeConflict
}
