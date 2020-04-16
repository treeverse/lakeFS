package merkle

import (
	"github.com/treeverse/lakefs/logging"
	"strings"
	"time"

	"github.com/treeverse/lakefs/db"

	"github.com/treeverse/lakefs/index/model"
)

func CompareEntries(a, b *model.Entry) (eqs int) {
	// names first
	eqs = strings.Compare(a.GetName(), b.GetName())
	// directories second
	if eqs == 0 && a.EntryType != b.EntryType {
		if a.EntryType < b.EntryType {
			eqs = -1
		} else if a.EntryType > b.EntryType {
			eqs = 1
		} else {
			eqs = 0
		}
	}
	return
}
func max(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func mergeChanges(current []*model.Entry, changes []*model.WorkspaceEntry) ([]*model.Entry, time.Time, error) {
	logger := logging.Default()
	merged := make([]*model.Entry, 0)
	var timeStamp time.Time
	nextCurrent := 0
	nextChange := 0
	for {
		// if both lists still have values, compare
		if nextChange < len(changes) && nextCurrent < len(current) {
			currEntry := current[nextCurrent]
			currChange := changes[nextChange]
			timeStamp = max(timeStamp, *currChange.EntryCreationDate)
			comparison := CompareEntries(currEntry, currChange.Entry())
			if comparison == 0 {
				// this is an override or deletion - do nothing

				// overwrite
				if !currChange.Tombstone {
					merged = append(merged, currChange.Entry())
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
				if currChange.Tombstone {
					logger.Error("trying to remove an entry that does not exist")
					return nil, timeStamp, db.ErrNotFound
				} else {
					merged = append(merged, currChange.Entry())
				}
			}
		} else if nextChange < len(changes) {
			// only changes left
			currChange := changes[nextChange]
			timeStamp = max(timeStamp, *currChange.EntryCreationDate)
			if currChange.Tombstone {
				logger.Error("trying to remove an entry that does not exist")
				return nil, timeStamp, db.ErrNotFound
			}
			merged = append(merged, currChange.Entry())
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
	return merged, timeStamp, nil
}
