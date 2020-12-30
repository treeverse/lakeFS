package onboard

import (
	"errors"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/treeverse/lakefs/catalog/rocks"
)

type inventoryEntryIterator struct {
	it    *InventoryIterator
	value *rocks.EntryRecord
	err   error
}

func NewValueToEntryIterator(it *InventoryIterator) rocks.EntryIterator {
	return &inventoryEntryIterator{
		it: it,
	}
}

func (e *inventoryEntryIterator) Next() bool {
	if e.err != nil {
		return false
	}
	hasNext := e.it.Next()
	if !hasNext {
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
	return true
}

var ErrIteratorNotSeakable = errors.New("can't seek on inventory iterator")

func (e *inventoryEntryIterator) SeekGE(_ rocks.Path) {
	e.err = ErrIteratorNotSeakable
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
