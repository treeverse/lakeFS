package onboard

import (
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

func CompareKeys(row1 *block.InventoryObject, row2 *block.InventoryObject) bool {
	if row1 == nil || row2 == nil {
		return false
	}
	return row1.Key < row2.Key
}

func CalcDiff(leftInv <-chan *block.InventoryObject, rightInv <-chan *block.InventoryObject) <-chan *ObjectImport {
	out := make(chan *ObjectImport)
	go func() {
		defer close(out)
		leftRow := <-leftInv
		rightRow := <-rightInv
		for leftRow != nil || rightRow != nil {
			if leftRow != nil && (rightRow == nil || CompareKeys(leftRow, rightRow)) {
				out <- &ObjectImport{Obj: *leftRow, ToDelete: true}
				leftRow = <-leftInv
			} else if leftRow == nil || CompareKeys(rightRow, leftRow) {
				out <- &ObjectImport{Obj: *rightRow}
				rightRow = <-rightInv
			} else if leftRow.Key == rightRow.Key {
				if leftRow.Checksum != rightRow.Checksum {
					out <- &ObjectImport{Obj: *rightRow}
				}
				leftRow = <-leftInv
				rightRow = <-rightInv
			}
		}
	}()
	return out
}

func CreateCommitMetadata(inv block.Inventory, stats InventoryImportStats) catalog.Metadata {
	return catalog.Metadata{
		"inventory_url":            inv.InventoryURL(),
		"source":                   inv.SourceName(),
		"added_or_changed_objects": strconv.Itoa(stats.AddedOrChanged),
		"deleted_objects":          strconv.Itoa(stats.Deleted),
	}
}

func ExtractInventoryURL(metadata catalog.Metadata) string {
	return metadata["inventory_url"]
}
