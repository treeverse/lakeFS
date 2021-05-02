package graveler

import (
	"bytes"
	"context"
	"errors"

	"github.com/treeverse/lakefs/pkg/logging"
)

type uncommittedDiffIterator struct {
	committedManager CommittedManager
	list             ValueIterator
	storageNamespace StorageNamespace
	metaRangeID      MetaRangeID
	value            *Diff
	err              error
	ctx              context.Context
}

// NewUncommittedDiffIterator lists uncommitted changes as a diff. If `metaRangeID` is empty then there is no commit and it returns all objects as added
func NewUncommittedDiffIterator(ctx context.Context, manager CommittedManager, list ValueIterator, sn StorageNamespace, metaRangeID MetaRangeID) DiffIterator {
	return &uncommittedDiffIterator{
		ctx:              ctx,
		committedManager: manager,
		list:             list,
		storageNamespace: sn,
		metaRangeID:      metaRangeID,
	}
}

func (d *uncommittedDiffIterator) getValueFromCommittedIfExists(val ValueRecord) (*Value, error) {
	if d.metaRangeID == "" {
		return nil, nil
	}
	value, err := d.committedManager.Get(d.ctx, d.storageNamespace, d.metaRangeID, val.Key)
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return value, nil
}

// getDiffType returns the diffType between value with committed
// Returns skip == true in case of no diff
func (d *uncommittedDiffIterator) getDiffType(val ValueRecord) (diffType DiffType, skip bool, err error) {
	committedVal, err := d.getValueFromCommittedIfExists(val)
	if err != nil {
		return 0, false, err
	}
	existsInCommitted := committedVal != nil
	if val.Value == nil {
		// tombstone
		if !existsInCommitted {
			logging.Default().
				WithFields(logging.Fields{"meta_range_id": d.metaRangeID, "storage_namespace": d.storageNamespace, "key": val.Key}).
				Warn("tombstone for a file that does not exist")
		}
		return DiffTypeRemoved, false, nil
	}
	if !existsInCommitted {
		return DiffTypeAdded, false, nil
	}
	if bytes.Equal(committedVal.Identity, val.Identity) {
		return 0, true, nil
	}
	return DiffTypeChanged, false, nil
}

func (d *uncommittedDiffIterator) Next() bool {
	for {
		if !d.list.Next() {
			d.value = nil
			return false
		}
		val := d.list.Value()
		diffType, skip, err := d.getDiffType(*val)
		if err != nil {
			d.value = nil
			d.err = err
			return false
		}
		if skip {
			continue
		}
		d.value = &Diff{
			Type:  diffType,
			Key:   val.Key,
			Value: val.Value,
		}
		return true
	}
}

func (d *uncommittedDiffIterator) SeekGE(id Key) {
	d.value = nil
	d.err = nil
	d.list.SeekGE(id)
}

func (d *uncommittedDiffIterator) Value() *Diff {
	return d.value
}

func (d *uncommittedDiffIterator) Err() error {
	return d.err
}

func (d *uncommittedDiffIterator) Close() {
	d.list.Close()
}
