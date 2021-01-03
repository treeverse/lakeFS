package committed

import (
	"bytes"
	"context"
	"fmt"

	"github.com/treeverse/lakefs/graveler"
)

type mergeIterator struct {
	diffIt       graveler.DiffIterator
	val          *graveler.ValueRecord
	base         graveler.MetaRangeID
	committedMgr graveler.CommittedManager
	ctx          context.Context
	ns           graveler.StorageNamespace
	err          error
}

// NewMergeIterator returns a ValueIterator with all changes to be performed on left in order to merge right into left, relative to base.
// The iterator will return ErrConflictFound when it reaches a conflict.
func NewMergeIterator(ctx context.Context, ns graveler.StorageNamespace, left, right, base graveler.MetaRangeID, committedMgr graveler.CommittedManager) (*mergeIterator, error) {
	diffIt, err := committedMgr.Diff(ctx, ns, left, right)
	if err != nil {
		return nil, err
	}
	return &mergeIterator{diffIt: diffIt, base: base, committedMgr: committedMgr}, nil
}

func (d *mergeIterator) Next() bool {
	for d.diffIt.Next() {
		val := d.diffIt.Value()
		key := val.Key
		typ := val.Type
		baseVal, err := d.committedMgr.Get(d.ctx, d.ns, d.base, key)
		if err != nil {
			d.err = fmt.Errorf("get from base tree: %w", err)
			return false
		}
		switch typ {
		case graveler.DiffTypeAdded:
			if baseVal == nil {
				d.setValue()
				return true
			}
			if !bytes.Equal(baseVal.Identity, val.Value.Identity) {
				d.err = graveler.ErrConflictFound
				return false
			}
			continue
		case graveler.DiffTypeChanged:
			if baseVal == nil {
				d.err = graveler.ErrConflictFound
				return false
			}
			if bytes.Equal(baseVal.Identity, val.Value.Identity) {
				continue // no change from base
			}
			if !bytes.Equal(baseVal.Identity, val.OldIdentity) {
				d.err = graveler.ErrConflictFound
				return false
			}
			d.setValue()
			return true
		case graveler.DiffTypeRemoved:
			if baseVal != nil {
				if bytes.Equal(baseVal.Identity, val.OldIdentity) {
					d.setValue()
					return true // removed
				}
				d.err = graveler.ErrConflictFound
			}
			// continue
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
