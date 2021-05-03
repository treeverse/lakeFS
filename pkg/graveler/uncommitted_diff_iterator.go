package graveler

import (
	"bytes"
	"context"

	"github.com/treeverse/lakefs/pkg/logging"
)

type uncommittedDiffIterator struct {
	committedList    ValueIterator
	uncommittedList  ValueIterator
	storageNamespace StorageNamespace
	metaRangeID      MetaRangeID
	value            *Diff
	err              error
	ctx              context.Context
}

// NewUncommittedDiffIterator lists uncommitted changes as a diff. If `metaRangeID` is empty then there is no commit and it returns all objects as added
func NewUncommittedDiffIterator(ctx context.Context, committedList ValueIterator, uncommittedList ValueIterator, sn StorageNamespace, metaRangeID MetaRangeID) DiffIterator {
	return &uncommittedDiffIterator{
		ctx:              ctx,
		committedList:    committedList,
		uncommittedList:  uncommittedList,
		storageNamespace: sn,
		metaRangeID:      metaRangeID,
	}
}

// getIdentityFromCommittedIfExists Returns the identity of the value if the value exists in the committed list
// Returns nil in case the value does not exist
func (d *uncommittedDiffIterator) getIdentityFromCommittedIfExists(val ValueRecord) ([]byte, error) {
	if d.committedList == nil {
		return nil, nil
	}
	d.committedList.SeekGE(val.Key)
	if d.committedList.Next() && bytes.Equal(d.committedList.Value().Key, val.Key) {
		return d.committedList.Value().Identity, nil
	}
	if d.committedList.Err() != nil {
		return nil, d.committedList.Err()
	}
	return nil, nil
}

// getDiffType returns the diffType between value with committed
// Returns skip == true in case of no diff
func (d *uncommittedDiffIterator) getDiffType(val ValueRecord) (diffType DiffType, skip bool, err error) {
	committedIdentity, err := d.getIdentityFromCommittedIfExists(val)
	if err != nil {
		return 0, false, err
	}
	existsInCommitted := committedIdentity != nil
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
	if bytes.Equal(committedIdentity, val.Identity) {
		return 0, true, nil
	}
	return DiffTypeChanged, false, nil
}

func (d *uncommittedDiffIterator) Next() bool {
	for {
		if !d.uncommittedList.Next() {
			d.value = nil
			return false
		}
		val := d.uncommittedList.Value()
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
	d.uncommittedList.SeekGE(id)
}

func (d *uncommittedDiffIterator) Value() *Diff {
	return d.value
}

func (d *uncommittedDiffIterator) Err() error {
	return d.err
}

func (d *uncommittedDiffIterator) Close() {
	d.uncommittedList.Close()
	if d.committedList != nil {
		d.committedList.Close()
	}
}
