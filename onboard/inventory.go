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

// CalcDiff returns a diff between two sorted arrays of InventoryObject
func CalcDiff(leftInv []block.InventoryObject, rightInv []block.InventoryObject) *InventoryDiff {
	res := InventoryDiff{}
	var leftIdx, rightIdx int
	for leftIdx < len(leftInv) || rightIdx < len(rightInv) {
		var leftRow, rightRow *block.InventoryObject
		if leftIdx < len(leftInv) {
			leftRow = &leftInv[leftIdx]
		}
		if rightIdx < len(rightInv) {
			rightRow = &rightInv[rightIdx]
		}
		if leftRow != nil && (rightRow == nil || CompareKeys(leftRow, rightRow)) {
			res.Deleted = append(res.Deleted, *leftRow)
			leftIdx++
		} else if leftRow == nil || CompareKeys(rightRow, leftRow) {
			res.AddedOrChanged = append(res.AddedOrChanged, *rightRow)
			rightIdx++
		} else if leftRow.Key == rightRow.Key {
			if leftRow.Checksum != rightRow.Checksum {
				res.AddedOrChanged = append(res.AddedOrChanged, *rightRow)
			}
			leftIdx++
			rightIdx++
		}
	}
	return &res
}

func CreateCommitMetadata(inv block.Inventory, diff InventoryDiff) catalog.Metadata {
	return catalog.Metadata{
		"inventory_url":            inv.InventoryURL(),
		"source":                   inv.SourceName(),
		"added_or_changed_objects": strconv.Itoa(len(diff.AddedOrChanged)),
		"deleted_objects":          strconv.Itoa(len(diff.Deleted)),
	}
}

func ExtractInventoryURL(metadata catalog.Metadata) string {
	return metadata["inventory_url"]
}
