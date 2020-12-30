package graveler

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/logging"
)

type uncommittedDiffIterator struct {
	committedManager CommittedManager
	list             ValueIterator
	storageNamespace StorageNamespace
	rangeID          MetaRangeID
	value            *Diff
	err              error
	ctx              context.Context
}

func NewUncommittedDiffIterator(ctx context.Context, manager CommittedManager, list ValueIterator, sn StorageNamespace, rangeID MetaRangeID) DiffIterator {
	return &uncommittedDiffIterator{
		ctx:              ctx,
		committedManager: manager,
		list:             list,
		storageNamespace: sn,
		rangeID:          rangeID,
	}
}

func (d *uncommittedDiffIterator) valueExistsInCommitted(val ValueRecord) (bool, error) {
	_, err := d.committedManager.Get(d.ctx, d.storageNamespace, d.rangeID, val.Key)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (d *uncommittedDiffIterator) getDiffType(val ValueRecord) (DiffType, error) {
	existsInCommitted, err := d.valueExistsInCommitted(val)
	if err != nil {
		return 0, err
	}

	if val.Value == nil {
		// tombstone
		if !existsInCommitted {
			logging.Default().
				WithFields(logging.Fields{"range_id": d.rangeID, "storage_namespace": d.storageNamespace, "key": val.Key}).
				Warn("tombstone for a file that does not exist")
		}
		return DiffTypeRemoved, nil
	}
	if existsInCommitted {
		return DiffTypeChanged, nil
	}
	return DiffTypeAdded, nil
}

func (d *uncommittedDiffIterator) Next() bool {
	if !d.list.Next() {
		d.value = nil
		return false
	}
	val := d.list.Value()
	diffType, err := d.getDiffType(*val)
	if err != nil {
		d.value = nil
		d.err = err
		return false
	}
	d.value = &Diff{
		Type:  diffType,
		Key:   val.Key,
		Value: val.Value,
	}
	return true
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
