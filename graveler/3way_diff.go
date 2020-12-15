package graveler

import (
	"bytes"
	"context"
)

type threeWayDiffIterator struct {
	diffIt       DiffIterator
	base         TreeID
	committedMgr CommittedManager
	ctx          context.Context
	ns           StorageNamespace
	err          error
}

func NewThreeWayDiffIterator(diffIt DiffIterator, base TreeID, committedMgr CommittedManager) *threeWayDiffIterator {
	return &threeWayDiffIterator{diffIt: diffIt, base: base, committedMgr: committedMgr}
}

func (d *threeWayDiffIterator) Next() bool {
	for d.diffIt.Next() {
		val := d.diffIt.Value()
		key := val.Key
		typ := val.Type
		baseVal, err := d.committedMgr.Get(d.ctx, d.ns, d.base, key)
		if err != nil {
			return false
		}
		switch typ {
		case DiffTypeAdded:
			if baseVal == nil {
				return true
			}
			if !bytes.Equal(baseVal.Identity, val.Value.Identity) {
				d.err = ErrConflictFound
				return false
			}
			return d.Next()
		case DiffTypeChanged:
			if baseVal == nil {
				d.err = ErrConflictFound
				return false
			}
			if bytes.Equal(baseVal.Identity, val.Value.Identity) {
				return d.Next() // no change from base
			}
			if !bytes.Equal(baseVal.Identity, val.OldIdentity) {
				d.err = ErrConflictFound
				return false
			}
			return true
		case DiffTypeRemoved:
			if baseVal != nil && bytes.Equal(baseVal.Identity, val.OldIdentity) {
				return true // removed
			}
			d.err = ErrConflictFound
			return false // conflict
		}
	}
	return false
}

func (d *threeWayDiffIterator) SeekGE(id Key) {
	d.diffIt.SeekGE(id)
}

func (d *threeWayDiffIterator) Value() *Diff {
	return d.diffIt.Value()
}

func (d *threeWayDiffIterator) Err() error {
	return d.err
}

func (d *threeWayDiffIterator) Close() {
	d.diffIt.Close()
}
