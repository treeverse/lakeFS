package graveler

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/logging"
)

type UncommittedDiffIterator struct {
	committedManager CommittedManager
	list             ValueIterator
	sn               StorageNamespace
	treeID           TreeID
	value            *Diff
	err              error
}

func newUncommittedDiffIterator(manager CommittedManager, list ValueIterator, sn StorageNamespace, treeItreeID TreeID) *UncommittedDiffIterator {
	return &UncommittedDiffIterator{
		committedManager: manager,
		list:             list,
		sn:               sn,
		treeID:           treeItreeID,
	}
}

func valueExistsInCommitted(ctx context.Context, committedManager CommittedManager, sn StorageNamespace, treeID TreeID, key Key) (bool, error) {
	_, err := committedManager.Get(ctx, sn, treeID, key)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func getDiffType(ctx context.Context, committedManager CommittedManager, sn StorageNamespace, treeID TreeID, key Key, tombstone bool) (DiffType, error) {
	existsInCommitted, err := valueExistsInCommitted(ctx, committedManager, sn, treeID, key)
	if err != nil {
		return 0, err
	}
	var diffType DiffType
	diffType = DiffTypeAdded
	if tombstone && existsInCommitted {
		diffType = DiffTypeRemoved
	}
	if tombstone && !existsInCommitted {
		logging.Default().WithFields(logging.Fields{"treeID": treeID, "stagingToken": sn, "key": key}).Warn("tombstone for file that does not exist")
		diffType = DiffTypeRemoved
	}
	if !tombstone && existsInCommitted {
		diffType = DiffTypeChanged
	}
	if !tombstone && !existsInCommitted {
		diffType = DiffTypeAdded
	}
	return diffType, nil
}

func (d *UncommittedDiffIterator) Next() bool {
	if !d.list.Next() {
		d.value = nil
		return false
	}
	val := d.list.Value()
	diffType, err := getDiffType(context.Background(), d.committedManager, d.sn, d.treeID, val.Key, val.Value == nil)
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

func (d *UncommittedDiffIterator) SeekGE(id Key) {
	d.value = nil
	d.list.SeekGE(id)
}

func (d *UncommittedDiffIterator) Value() *Diff {
	return d.value
}

func (d *UncommittedDiffIterator) Err() error {
	return d.err
}

func (d *UncommittedDiffIterator) Close() {
	d.list.Close()
}
