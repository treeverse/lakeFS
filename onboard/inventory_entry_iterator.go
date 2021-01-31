package onboard

import (
	"errors"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/cmdutils"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type inventoryEntryIterator struct {
	it       *InventoryIterator
	value    *catalog.EntryRecord
	err      error
	progress *cmdutils.Progress
}

func NewValueToEntryIterator(it *InventoryIterator, progress *cmdutils.Progress) catalog.EntryIterator {
	return &inventoryEntryIterator{
		it:       it,
		progress: progress,
	}
}

var (
	ErrIteratorNotSeekable = errors.New("can't seek on inventory iterator")
)

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

	e.value = &catalog.EntryRecord{
		Path: catalog.Path(v.Obj.Key),
		Entry: &catalog.Entry{
			Address: v.Obj.PhysicalAddress,
			Size:    v.Obj.Size,
			ETag:    v.Obj.Checksum,
		},
	}

	if v.Obj.LastModified != nil {
		e.value.LastModified = timestamppb.New(*v.Obj.LastModified)
	}

	e.progress.Add(1)
	return true
}

func (e *inventoryEntryIterator) SeekGE(_ catalog.Path) {
	e.value = nil
	e.err = ErrIteratorNotSeekable
}

func (e *inventoryEntryIterator) Value() *catalog.EntryRecord {
	return e.value
}

func (e *inventoryEntryIterator) Err() error {
	return e.err
}

func (e *inventoryEntryIterator) Close() {
	// nothing to do
}
