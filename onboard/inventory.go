package onboard

import (
	"fmt"
	"strconv"
	"time"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
)

type InventoryDiff struct {
	DryRun               bool
	AddedOrChanged       []block.InventoryObject
	Deleted              []block.InventoryObject
	PreviousInventoryURL string
	PreviousImportDate   time.Time
}
type ImportObject struct {
	Obj       block.InventoryObject
	IsDeleted bool
	IsChanged bool
}

type Iterator interface {
	Next() bool
	Err() error
	Get() ImportObject
}
type DiffIterator struct {
	leftInv   block.InventoryIterator
	rightInv  block.InventoryIterator
	leftNext  bool
	rightNext bool
	value     ImportObject
	err       error
}

// onboard.InventoryIterator reads from block.InventoryIterator and converts the objects to ImportObject
type InventoryIterator struct {
	block.InventoryIterator
}

func NewInventoryIterator(it block.InventoryIterator) *InventoryIterator {
	return &InventoryIterator{InventoryIterator: it}
}

func (s *InventoryIterator) Get() ImportObject {
	return ImportObject{
		Obj:       *s.InventoryIterator.Get(),
		IsDeleted: false,
	}
}

func NewDiffIterator(leftInv block.InventoryIterator, rightInv block.InventoryIterator) Iterator {
	res := &DiffIterator{leftInv: leftInv, rightInv: rightInv}
	res.leftNext = leftInv.Next()
	res.rightNext = rightInv.Next()
	return res
}

func (d *DiffIterator) Next() bool {
	for {
		if !d.leftNext && d.leftInv.Err() != nil {
			d.err = fmt.Errorf("failed to get value from left inventory: %w", d.leftInv.Err())
			return false
		}
		if !d.rightNext && d.rightInv.Err() != nil {
			d.err = fmt.Errorf("failed to get value from right inventory: %w", d.rightInv.Err())
			return false
		}
		if !d.rightNext && !d.leftNext {
			return false
		}
		switch {
		case d.leftNext && (!d.rightNext || CompareKeys(d.leftInv.Get(), d.rightInv.Get())):
			d.value = ImportObject{Obj: *d.leftInv.Get(), IsDeleted: true}
			d.leftNext = d.leftInv.Next()
			return true
		case !d.leftNext || CompareKeys(d.rightInv.Get(), d.leftInv.Get()):
			d.value = ImportObject{Obj: *d.rightInv.Get()}
			d.rightNext = d.rightInv.Next()
			return true
		case d.leftInv.Get().Key == d.rightInv.Get().Key:
			if d.leftInv.Get().Checksum != d.rightInv.Get().Checksum {
				d.value = ImportObject{Obj: *d.rightInv.Get(), IsChanged: true}
				d.leftNext = d.leftInv.Next()
				d.rightNext = d.rightInv.Next()
				return true
			}
			d.leftNext = d.leftInv.Next()
			d.rightNext = d.rightInv.Next()
		}
	}
}

func (d *DiffIterator) Err() error {
	return d.err
}

func (d *DiffIterator) Get() ImportObject {
	return d.value
}

func CompareKeys(row1 *block.InventoryObject, row2 *block.InventoryObject) bool {
	if row1 == nil || row2 == nil {
		return false
	}
	return row1.Key < row2.Key
}

func CreateCommitMetadata(inv block.Inventory, stats InventoryImportStats) catalog.Metadata {
	return catalog.Metadata{
		"inventory_url":            inv.InventoryURL(),
		"source":                   inv.SourceName(),
		"added_or_changed_objects": strconv.Itoa(int(*stats.AddedOrChanged)),
		"deleted_objects":          strconv.Itoa(int(*stats.Deleted)),
	}
}

func ExtractInventoryURL(metadata catalog.Metadata) string {
	return metadata["inventory_url"]
}
