package index

import "github.com/treeverse/lakefs/index/model"

func CombineWithTreeEntries(treeEntries []*model.Entry, workspaceEntries []*model.WorkspaceEntry, amount int) []*model.Entry {
	tI := 0
	wI := 0
	wEntry := workspaceEntries[wI]
	tEntry := treeEntries[tI]
	var result []*model.Entry
	for amount > 0 {
		if wEntry.Path < tEntry.Name {
			amount--
			result = append(result, wEntry.Entry())
			wI++
		} else if wEntry.Path > tEntry.Name {
			amount--
			result = append(result, tEntry)
			tI++
		} else {
			if !wEntry.Tombstone {
				result = append(result, wEntry.Entry())
				amount--
			}
			tI++
			wI++
		}
	}
	return result
}
