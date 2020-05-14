package index

import (
	"errors"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/index/merkle"
	"github.com/treeverse/lakefs/index/model"
	"github.com/treeverse/lakefs/index/store"
)

func WorkspaceDiff(tx store.RepoOperations, branch string) (merkle.Differences, error) {
	var result merkle.Differences
	branchData, err := tx.ReadBranch(branch)
	if err != nil {
		return nil, err
	}
	tree := merkle.New(branchData.CommitRoot)
	root := &model.Entry{
		Name:      "",
		Address:   branchData.CommitRoot,
		EntryType: model.EntryTypeTree,
		Checksum:  "",
	}
	_, err = diffRecursive(tx, branch, nil, root, *tree, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func diffRecursive(tx store.RepoOperations, branch string, wsEntry *model.WorkspaceEntry, entry *model.Entry, tree merkle.Merkle, diff *merkle.Differences) (string, error) {
	if entry == nil {
		*diff = append(*diff, merkle.Difference{Type: merkle.DifferenceTypeAdded, Direction: merkle.DifferenceDirectionLeft, Path: wsEntry.Path, PathType: *wsEntry.EntryType})
		return "ADDED", nil
	}
	//entry, err := tx.ReadTreeEntry(addr, *wsEntry.EntryName)
	//if err != nil {
	//	if errors.Is(err, db.ErrNotFound) && !wsEntry.Tombstone {
	//
	//	}
	//	return "", err
	//}
	if wsEntry != nil && *wsEntry.EntryType == model.EntryTypeObject {
		// OBJECT
		if entry.Checksum != *wsEntry.EntryChecksum {
			*diff = append(*diff, merkle.Difference{Type: merkle.DifferenceTypeChanged, Direction: merkle.DifferenceDirectionLeft, Path: wsEntry.Path, PathType: model.EntryTypeObject})
			return "CHANGED", nil
		}
		if wsEntry.Tombstone {
			return "DELETED", nil
		}
		// NO CHANGE -- return
	} else {
		wsPath := ""
		if wsEntry != nil {
			wsPath = wsEntry.Path
		}
		wsEntriesInDir, _, err := tx.ListWorkspaceDirectory(branch, wsPath, "", -1)

		if err != nil {
			return "", err
		}
		allDeleted := true
		var deleted []*model.WorkspaceEntry
		for _, currentWsEntry := range wsEntriesInDir {
			currentEntry, err := tx.ReadTreeEntry(entry.Address, *currentWsEntry.EntryName)
			if errors.Is(err, db.ErrNotFound) {
				currentEntry = nil
			} else if err != nil {
				return "", nil
			}
			diffType, err := diffRecursive(tx, branch, currentWsEntry, currentEntry, tree, diff)
			if err != nil {
				return "", err
			}
			if diffType != "DELETED" {
				allDeleted = false
			} else {
				deleted = append(deleted, currentWsEntry)
			}
		}
		if allDeleted && wsEntry != nil && entry.ObjectCount == wsEntry.TombstoneCount {
			return "DELETED", nil
		} else {
			for _, deletedEntry := range deleted {
				*diff = append(*diff, merkle.Difference{
					Type:      merkle.DifferenceTypeRemoved,
					Direction: merkle.DifferenceDirectionLeft,
					Path:      deletedEntry.Path,
					PathType:  *deletedEntry.EntryType,
				})
			}
		}
	}
	return "", nil
}
