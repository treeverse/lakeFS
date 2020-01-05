package merkle

import (
	"strings"
	"treeverse-lake/index/model"
)

type entryLike interface {
	GetType() model.Entry_Type
	GetName() string
	GetAddress() string
}

func compareEntries(a, b entryLike) (eqs int) {
	// names first
	eqs = strings.Compare(a.GetName(), b.GetName())
	// directories second
	if eqs == 0 && a.GetType() != b.GetType() {
		if a.GetType() < b.GetType() {
			eqs = -1
		} else if a.GetType() > b.GetType() {
			eqs = 1
		} else {
			eqs = 0
		}
	}
	return
}

func mergeChanges(current []*model.Entry, changes []*change) []*model.Entry {
	merged := make([]*model.Entry, 0)
	nextCurrent := 0
	nextChange := 0
	for {
		// if both lists still have values, compare
		if nextChange < len(changes) && nextCurrent < len(current) {
			currEntry := current[nextCurrent]
			currChange := changes[nextChange]
			comparison := compareEntries(currEntry, currChange)
			if comparison == 0 {
				// this is an override or deletion - do nothing

				// overwrite
				if !currChange.Tombstone {
					merged = append(merged, currChange.AsEntry())
				}
				// otherwise, skip both
				nextCurrent++
				nextChange++
			} else if comparison == -1 {
				nextCurrent++
				// current entry comes first
				merged = append(merged, currEntry)
			} else {
				nextChange++
				// changed entry comes first
				merged = append(merged, currChange.AsEntry())
			}
		} else if nextChange < len(changes) {
			// only changes left
			currChange := changes[nextChange]
			if currChange.Tombstone {
				// this is an override or deletion
				nextChange++
				continue // remove.
			}
			merged = append(merged, currChange.AsEntry())
			nextChange++
		} else if nextCurrent < len(current) {
			// only current entries left
			currEntry := current[nextCurrent]
			merged = append(merged, currEntry)
			nextCurrent++
		} else {
			// done with both
			break
		}
	}
	return merged
}
