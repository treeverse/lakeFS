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
	metaRangeID      MetaRangeID
	value            *Diff
	err              error
	ctx              context.Context
}

// NewUncommittedDiffIterator list uncommitted changes as diff. The `metaRangeID` can be empty in case there is no commit
func NewUncommittedDiffIterator(ctx context.Context, manager CommittedManager, list ValueIterator, sn StorageNamespace, metaRangeID MetaRangeID) DiffIterator {
	return &uncommittedDiffIterator{
		ctx:              ctx,
		committedManager: manager,
		list:             list,
		storageNamespace: sn,
		metaRangeID:      metaRangeID,
	}
}

func (d *uncommittedDiffIterator) valueExistsInCommitted(val ValueRecord) (bool, error) {
	if d.metaRangeID == "" {
		return false, nil
	}
	_, err := d.committedManager.Get(d.ctx, d.storageNamespace, d.metaRangeID, val.Key)
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
				WithFields(logging.Fields{"meta_range_id": d.metaRangeID, "storage_namespace": d.storageNamespace, "key": val.Key}).
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
