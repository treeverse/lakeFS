package onboard

import (
	"errors"

	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/cmdutils"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type inventoryEntryIterator struct {
	it       *InventoryIterator
	value    *rocks.EntryRecord
	err      error
	progress *cmdutils.Progress
}

func NewValueToEntryIterator(it *InventoryIterator, progress *cmdutils.Progress) rocks.EntryIterator {
	return &inventoryEntryIterator{
		it:       it,
		progress: progress,
	}
}

func (e *inventoryEntryIterator) Next() bool {
	if e.err != nil {
		return false
	}
	if !e.it.Next() {
		e.value = nil
		e.err = e.it.Err()
		return false
	}
	v := e.it.Get()

	e.value = &rocks.EntryRecord{
		Path: rocks.Path(v.Obj.Key),
		Entry: &rocks.Entry{
			Address: v.Obj.PhysicalAddress,
			Size:    v.Obj.Size,
			ETag:    []byte(v.Obj.Checksum),
		},
	}

	if v.Obj.LastModified != nil {
		e.value.LastModified = timestamppb.New(*v.Obj.LastModified)
	}

	e.progress.Add(1)
	return true
}

var ErrIteratorNotSeekable = errors.New("can't seek on inventory iterator")

func (e *inventoryEntryIterator) SeekGE(_ rocks.Path) {
	e.err = ErrIteratorNotSeekable
}

func (e *inventoryEntryIterator) Value() *rocks.EntryRecord {
	return e.value
}

func (e *inventoryEntryIterator) Err() error {
	return e.err
}

func (e *inventoryEntryIterator) Close() {
	// nothing to do
}
