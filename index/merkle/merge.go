package merkle

import (
	"strings"

	"github.com/treeverse/lakefs/db"

	log "github.com/sirupsen/logrus"

	"github.com/treeverse/lakefs/index/model"
)

func CompareEntries(a, b *model.Entry) (eqs int) {
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
func max(a int64, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func mergeChanges(current []*model.Entry, changes []*model.WorkspaceEntry) ([]*model.Entry, int64, error) {
	merged := make([]*model.Entry, 0)
	var timeStamp int64
	nextCurrent := 0
	nextChange := 0
	for {
		// if both lists still have values, compare
		if nextChange < len(changes) && nextCurrent < len(current) {
			currEntry := current[nextCurrent]
			currChange := changes[nextChange]
			timeStamp = max(timeStamp, currChange.GetEntry().GetTimestamp())
			comparison := CompareEntries(currEntry, currChange.GetEntry())
			if comparison == 0 {
				// this is an override or deletion - do nothing

				// overwrite
				if !currChange.GetTombstone() {
					merged = append(merged, currChange.GetEntry())
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
					log.Error("trying to remove an entry that does not exist")
					return nil, 0, db.ErrNotFound
				} else {
					merged = append(merged, currChange.GetEntry())
				}
			}
		} else if nextChange < len(changes) {
			// only changes left
			currChange := changes[nextChange]
			timeStamp = max(timeStamp, currChange.GetEntry().GetTimestamp())
			if currChange.GetTombstone() {
				log.Error("trying to remove an entry that does not exist")
				return nil, 0, db.ErrNotFound
			}
			merged = append(merged, currChange.GetEntry())
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
