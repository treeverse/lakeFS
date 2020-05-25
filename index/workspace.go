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
		err = diffRecursive(tx, branch, "", branchData.CommitRoot, *tree, &result)
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
func diffRecursive(tx store.RepoOperations, branch, parentPath, parentAddress string, tree merkle.Merkle, result *merkle.Differences) error {
	wsEntriesInDir, _, err := tx.ListWorkspaceDirectory(branch, parentPath, "", "", -1)
	if err != nil {
		return err
	}
	for _, currentWsEntry := range wsEntriesInDir {
		currentEntry, err := tx.ReadTreeEntry(parentAddress, *currentWsEntry.EntryName)
		if errors.Is(err, db.ErrNotFound) {
			// entry doesn't exist in tree
			currentEntry = nil
		} else if err != nil {
			return err
		}
		if currentEntry == nil {
			// added
			*result = append(*result, merkle.Difference{Type: merkle.DifferenceTypeAdded, Direction: merkle.DifferenceDirectionLeft, Path: currentWsEntry.Path, PathType: *currentWsEntry.EntryType})
		} else if currentWsEntry.TombstoneCount == currentEntry.ObjectCount {
			// deleted
			*result = append(*result, merkle.Difference{Type: merkle.DifferenceTypeRemoved, Direction: merkle.DifferenceDirectionLeft, Path: currentWsEntry.Path, PathType: *currentWsEntry.EntryType})
		} else if *currentWsEntry.EntryType == model.EntryTypeObject {
			// object: check if was changed
			if currentWsEntry.TombstoneCount == 0 && currentEntry.Checksum != *currentWsEntry.EntryChecksum {
				*result = append(*result, merkle.Difference{Type: merkle.DifferenceTypeChanged, Direction: merkle.DifferenceDirectionLeft, Path: currentWsEntry.Path, PathType: model.EntryTypeObject})
			}
		} else {
			// directory: dive in
			err = diffRecursive(tx, branch, currentWsEntry.Path, currentEntry.Address, tree, result)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
