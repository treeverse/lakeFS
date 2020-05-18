package index

import (
	"errors"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/index/merkle"
	"github.com/treeverse/lakefs/index/model"
	"github.com/treeverse/lakefs/index/store"
)

func (index *DBIndex) DiffWorkspace(repoId, branch string) (merkle.Differences, error) {
	res, err := index.store.RepoTransact(repoId, func(tx store.RepoOperations) (i interface{}, err error) {
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
	})
	if err != nil {
		index.log().WithError(err).WithField("branch", branch).Error("could not do workspace diff")
		return nil, err
	}
	return res.(merkle.Differences), nil
}

// diffRecursive scans the workspace recursively and compares it to entries in the tree.
// It starts with the given WorkspaceEntry, and accumulates the diff in the result array.
// It backpropogates whether or not the given entry was deleted, and adds the uppermost deleted entry to the result.
func diffRecursive(tx store.RepoOperations, branch string, wsEntry *model.WorkspaceEntry, entry *model.Entry, tree merkle.Merkle, result *merkle.Differences) (isDeleted bool, err error) {
	if entry == nil {
		// Entry doesn't exist in tree - it was added
		*result = append(*result, merkle.Difference{Type: merkle.DifferenceTypeAdded, Direction: merkle.DifferenceDirectionLeft, Path: wsEntry.Path, PathType: *wsEntry.EntryType})
		return false, nil
	}
	if wsEntry != nil && *wsEntry.EntryType == model.EntryTypeObject {
		// OBJECT
		if wsEntry.TombstoneCount == 0 && entry.Checksum != *wsEntry.EntryChecksum {
			*result = append(*result, merkle.Difference{Type: merkle.DifferenceTypeChanged, Direction: merkle.DifferenceDirectionLeft, Path: wsEntry.Path, PathType: model.EntryTypeObject})
			return false, nil
		}
		if wsEntry.TombstoneCount >= 1 {
			return true, nil
		}
	}
	// DIRECTORY
	wsPath := ""
	if wsEntry != nil {
		wsPath = wsEntry.Path
	}
	wsEntriesInDir, _, err := tx.ListWorkspaceDirectory(branch, wsPath, "", "", -1)

	if err != nil {
		return false, err
	}
	var deletedEntries []*model.WorkspaceEntry
	for _, currentWsEntry := range wsEntriesInDir {
		currentEntry, err := tx.ReadTreeEntry(entry.Address, *currentWsEntry.EntryName)
		if errors.Is(err, db.ErrNotFound) {
			// entry doesn't exist in tree
			currentEntry = nil
		} else if err != nil {
			return false, nil
		}
		isDeleted, err := diffRecursive(tx, branch, currentWsEntry, currentEntry, tree, result)
		if err != nil {
			return false, err
		}
		if isDeleted {
			deletedEntries = append(deletedEntries, currentWsEntry)
		}
	}
	if len(deletedEntries) == len(wsEntriesInDir) && wsEntry != nil && entry.ObjectCount == wsEntry.TombstoneCount {
		// All entries under this directory were deleted, mark as deleted
		return true, nil
	} else {
		// This directory was not deleted, add its deleted children to result
		for _, deletedEntry := range deletedEntries {
			*result = append(*result, merkle.Difference{
				Type:      merkle.DifferenceTypeRemoved,
				Direction: merkle.DifferenceDirectionLeft,
				Path:      deletedEntry.Path,
				PathType:  *deletedEntry.EntryType,
			})
		}
	}
	return false, nil
}
