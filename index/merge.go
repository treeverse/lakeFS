package index

import (
	"sort"
	"strings"
	"versio-index/ident"
	"versio-index/index/model"
	"versio-index/index/path"

	"golang.org/x/xerrors"
)

type change struct {
	changeType model.Entry_Type
	isDeletion bool
	name       string

	object *model.Object
}

func groupChangeSet(entries []*model.WorkspaceEntry) (grouped map[string][]*change, depthIndex []string) {
	grouped = make(map[string][]*change)
	for _, entry := range entries {
		p := path.New(entry.GetPath())
		treePath, name := p.Pop()
		if _, exists := grouped[treePath.String()]; !exists {
			grouped[treePath.String()] = make([]*change, 0)
		}
		// write either a tombstone or an entry
		if entry.GetTombstone() != nil {
			grouped[treePath.String()] = append(grouped[treePath.String()], &change{
				changeType: model.Entry_BLOB,
				isDeletion: true,
				name:       name,
			})
		} else {
			grouped[treePath.String()] = append(grouped[treePath.String()], &change{
				changeType: model.Entry_BLOB,
				isDeletion: false,
				name:       name,
				object:     entry.GetObject(),
			})
		}

		// ascend up the tree
		done := false
		for !done {
			treePath, name = p.Pop()
			if _, exists := grouped[treePath.String()]; !exists {
				grouped[treePath.String()] = make([]*change, 0)
			}
			grouped[treePath.String()] = append(grouped[treePath.String()], &change{
				changeType: model.Entry_TREE,
				isDeletion: false,
				name:       name,
			})
			if treePath != nil {
				done = true
			}
		}
	}

	// iterate over groups, starting with the deeper ones and going up
	depthIndex = make([]string, len(grouped))
	i := 0
	for key, _ := range grouped {
		depthIndex[i] = key
		i++
	}
	sort.SliceStable(depthIndex, func(i, j int) bool {
		return len(path.New(depthIndex[i]).SplitParts()) > len(path.New(depthIndex[j]).SplitParts())
	})
	return
}

func partialCommit(tx Transaction, repo *model.Repo, branch string) (string, error) {
	var empty string

	// 0. read current branch
	branchData, err := getOrCreateBranch(tx, repo, branch)
	if err != nil {
		return empty, err
	}

	// 1. iterate all changes in the current workspace
	wsEntries, err := tx.ListWorkspace(branch)
	if err != nil {
		return empty, err
	}

	// 2. group by containing tree
	changes, depthIndex := groupChangeSet(wsEntries)

	// 4. Iterate all changes starting with where the blobs are
	for _, containingPath := range depthIndex {
		// iterate and merge current tree (if exist) with list of entries
		dir, err := traverseDir(tx, branchData.GetWorkspaceRoot(), containingPath)
		var mergedEntries []*model.Entry
		if xerrors.Is(err, ErrNotFound) {
			// no such tree yet, let's create one.
			mergedEntries = partialCommitMergeTree(make([]*model.Entry, 0), changes[containingPath])
		} else if err != nil {
			return empty, err
		} else {
			currentEntries, err := tx.ListEntries(dir)
			if err != nil {
				return empty, err
			}
			mergedEntries = partialCommitMergeTree(currentEntries, changes[containingPath])
		}
		// write new tree
		_, name := path.New(containingPath).Pop()
		addr := hashEntries(mergedEntries)

		// TODO: this is wrong?
		err = tx.WriteEntry(addr, "d", name, &model.Entry{
			Name:    name,
			Address: addr,
			Type:    model.Entry_TREE,
		})
		if err != nil {
			return empty, err
		}
	}
	return empty, nil
}

func hashEntries(entries []*model.Entry) string {
	hashes := make([]string, len(entries))
	for i, ent := range entries {
		hashes[i] = ident.Hash(ent)
	}
	return ident.MultiHash(hashes...)
}

func partialCommitMergeTree(currentEntries []*model.Entry, changes []*change) []*model.Entry {
	// iterate sorted lists, merging them
	combinedEntries := make([]*model.Entry, 0)
	currentInd := 0
	changesInd := 0
	var nextCurrent *model.Entry
	var nextChange *change
	for {
		if currentInd >= len(currentEntries) && changesInd >= len(changes) {
			break // done with both lists
		}

		if currentInd < len(currentEntries) {
			nextCurrent = currentEntries[currentInd]
		} else {
			nextCurrent = nil
		}
		if changesInd < len(changes) {
			nextChange = changes[changesInd]
		} else {
			nextChange = nil
		}

		if nextCurrent != nil && nextChange != nil {
			// compare them
			compareResult := strings.Compare(nextCurrent.Name, nextChange.name)
			if compareResult == 0 {
				// existing path!
				currentInd++ // we don't need the old value
				changesInd++
				if nextChange.isDeletion {
					// this is a deletion, simply skip entry
					// TODO: GC
					continue
				} else {
					// this is an overwrite
					combinedEntries = append(combinedEntries, &model.Entry{
						Name:      nextChange.name,
						Address:   ident.Hash(nextChange.object),
						Type:      model.Entry_BLOB,
						Metadata:  nextChange.object.GetMetadata(),
						Timestamp: nextChange.object.GetTimestamp(),
					})
				}
			} else if compareResult == -1 {
				// old is bigger, push new
				if !nextChange.isDeletion { // makes no sense otherwise
					combinedEntries = append(combinedEntries, &model.Entry{
						Name:      nextChange.name,
						Address:   ident.Hash(nextChange.object),
						Type:      model.Entry_BLOB,
						Metadata:  nextChange.object.GetMetadata(),
						Timestamp: nextChange.object.GetTimestamp(),
					})
				}
				changesInd++
			} else {
				// new is bigger, push old
				combinedEntries = append(combinedEntries, nextCurrent)
				currentInd++
			}
		} else if nextCurrent != nil {
			// we only have existing entries left, we'll just append them
			combinedEntries = append(combinedEntries, nextCurrent)
			currentInd++
		} else if nextChange != nil {
			// we only have new entries left, done with original tree
			if !nextChange.isDeletion { // makes no sense otherwise
				combinedEntries = append(combinedEntries, &model.Entry{
					Name:      nextChange.name,
					Address:   ident.Hash(nextChange.object),
					Type:      model.Entry_BLOB,
					Metadata:  nextChange.object.GetMetadata(),
					Timestamp: nextChange.object.GetTimestamp(),
				})
			} // no else, we discard tombstones for stuff we didnt have before
			changesInd++
		} else {
			// both are done!!!
			break
		}
	}
	return combinedEntries
}

func gc(tx Transaction, treeAddress string) {

}
