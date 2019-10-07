package index

import (
	"math/rand"
	"strings"
	"time"
	"versio-index/ident"
	"versio-index/index/model"

	"golang.org/x/xerrors"
)

const (
	PathSeparator = "/"
)

func writeEntryToWorkspace(tx Transaction, repo *model.Repo, branch, path string, entry *model.WorkspaceEntry) error {
	err := tx.WriteToWorkspacePath(branch, path, entry)
	if err != nil {
		return err
	}
	if shouldPartiallyCommit(repo) {
		_, err = partialCommit(tx, repo, branch)
		if err != nil {
			return err
		}
	}
	return nil
}

func resolveReadRoot(tx ReadOnlyTransaction, repo *model.Repo, branch string) (string, error) {
	var empty string
	branchData, err := tx.ReadBranch(branch)
	if xerrors.Is(err, ErrNotFound) {
		// fallback to default branch
		branchData, err = tx.ReadBranch(repo.DefaultBranch)
		if err != nil {
			return empty, err
		}
	} else if err != nil {
		return empty, err // unexpected error
	}
	if strings.EqualFold(branchData.GetWorkspaceRoot(), "") {
		return branchData.GetCommitRoot(), nil
	}
	return branchData.GetWorkspaceRoot(), nil
}

func readFromTree(tx ReadOnlyTransaction, repo *model.Repo, branch, path string) (*model.Object, error) {
	// resolve tree root to read from
	root, err := resolveReadRoot(tx, repo, branch)
	if err != nil {
		return nil, err
	}
	// get the tree
	return traverse(tx, root, path)
}

func traverse(tx ReadOnlyTransaction, treeID, path string) (*model.Object, error) {
	currentAddress := treeID
	parts := strings.Split(path, PathSeparator)
	for i, part := range parts {
		if i == len(parts)-1 {
			// last item in the path is the blob
			entry, err := tx.ReadEntry(currentAddress, "f", part)
			if err != nil {
				return nil, err
			}
			blob, err := tx.ReadBlob(entry.GetAddress())
			if err != nil {
				return nil, err
			}
			return &model.Object{
				Blob:     blob,
				Metadata: entry.GetMetadata(),
			}, nil
		}
		entry, err := tx.ReadEntry(currentAddress, "d", part)
		if err != nil {
			return nil, err
		}
		currentAddress = entry.GetAddress()
	}
	return nil, ErrNotFound
}

func traverseDir(tx ReadOnlyTransaction, treeID, path string) (string, error) {
	currentAddress := treeID
	parts := strings.Split(path, PathSeparator)
	for _, part := range parts {
		entry, err := tx.ReadEntry(currentAddress, "d", part)
		if err != nil {
			return "", err
		}
		currentAddress = entry.GetAddress()
	}
	return currentAddress, nil
}

func shouldPartiallyCommit(repo *model.Repo) bool {
	chosen := rand.Float32()
	return chosen < repo.GetPartialCommitRatio()
}

func partialCommit(tx Transaction, repo *model.Repo, branch string) (string, error) {
	var empty string
	// 1. iterate all changes in the current workspace
	entries, err := tx.ListEntries(branch)
	if err != nil {
		return empty, err
	}

	// group by containing tree

	// calc and write all changed trees

	//

	// 2. Apply them to the Merkle root as exists in the branch pointer
	// 3. calculate new Merkle root
	// 4. save it in the branch pointer
	return "", nil
}

func gc(tx Transaction, treeAddress string) {

}

type Index struct {
	store Store
}

func NewIndex(store Store) *Index {
	return &Index{
		store: store,
	}
}

// Business logic

func (index *Index) Read(repo *model.Repo, branch, path string) (*model.Object, error) {
	obj, err := index.store.ReadTransact(repo, func(tx ReadOnlyTransaction) (interface{}, error) {
		var obj *model.Object
		we, err := tx.ReadFromWorkspace(branch, path)
		if err != nil && !xerrors.Is(err, ErrNotFound) {
			// an actual error has occurred, return it.
			return nil, err
		}
		if we.GetTombstone() != nil {
			// object was deleted deleted
			return nil, ErrNotFound
		}
		if xerrors.Is(err, ErrNotFound) {
			// not in workspace, let's try reading it from branch tree
			obj, err = readFromTree(tx, repo, branch, path)
		}
		// we found an object in the workspace
		obj = we.GetObject()
		return obj, nil
	})
	if err != nil {
		return nil, err
	}
	return obj.(*model.Object), nil
}

func (index *Index) Write(repo *model.Repo, branch, path string, object *model.Object) error {
	_, err := index.store.Transact(repo, func(tx Transaction) (interface{}, error) {
		err := writeEntryToWorkspace(tx, repo, branch, path, &model.WorkspaceEntry{
			Data: &model.WorkspaceEntry_Object{Object: object},
		})
		return nil, err
	})
	return err
}

func (index *Index) Delete(repo *model.Repo, branch, path string) error {
	_, err := index.store.Transact(repo, func(tx Transaction) (interface{}, error) {
		err := writeEntryToWorkspace(tx, repo, branch, path, &model.WorkspaceEntry{
			Data: &model.WorkspaceEntry_Tombstone{Tombstone: &model.Tombstone{}},
		})
		return nil, err
	})
	return err
}

func (index *Index) List(repo *model.Repo, branch, path string) ([]*model.Entry, error) {
	entries, err := index.store.Transact(repo, func(tx Transaction) (interface{}, error) {
		_, err := partialCommit(tx, repo, branch)
		if err != nil {
			return nil, err
		}

		root, err := resolveReadRoot(tx, repo, branch)
		if err != nil {
			return nil, err
		}
		addr, err := traverseDir(tx, root, path)
		if err != nil {
			return nil, err
		}
		return tx.ListEntries(addr)
	})
	if err != nil {
		return nil, err
	}
	return entries.([]*model.Entry), nil
}

func (index *Index) Reset(repo *model.Repo, branch string) error {
	// clear workspace, set branch workspace root back to commit root
	_, err := index.store.Transact(repo, func(tx Transaction) (interface{}, error) {
		tx.ClearWorkspace(branch)
		branchData, err := tx.ReadBranch(branch)
		if err != nil {
			return nil, err
		}
		gc(tx, branchData.GetWorkspaceRoot())
		branchData.WorkspaceRoot = branchData.GetCommitRoot()
		return nil, tx.WriteBranch(branch, branchData)
	})
	return err
}

func (index *Index) Commit(repo *model.Repo, branch, message, committer string, metadata map[string]string) error {
	_, err := index.store.Transact(repo, func(tx Transaction) (interface{}, error) {
		root, err := partialCommit(tx, repo, branch)
		if err != nil {
			return nil, err
		}
		branchData, err := tx.ReadBranch(branch)
		if err != nil {
			return nil, err
		}
		commit := &model.Commit{
			Tree:      root,
			Parents:   []string{branchData.GetCommit()},
			Committer: committer,
			Message:   message,
			Timestamp: time.Now().Unix(),
			Metadata:  metadata,
		}
		commitAddr := ident.Hash(commit)
		err = tx.WriteCommit(commitAddr, commit)
		if err != nil {
			return nil, err
		}
		branchData.Commit = commitAddr
		branchData.CommitRoot = commit.GetTree()
		branchData.WorkspaceRoot = commit.GetTree()

		return nil, tx.WriteBranch(branch, branchData)
	})
	return err
}

func (index *Index) DeleteBranch(repo *model.Repo, branch string) error {
	_, err := index.store.Transact(repo, func(tx Transaction) (interface{}, error) {
		branchData, err := tx.ReadBranch(branch)
		if err != nil {
			return nil, err
		}
		tx.ClearWorkspace(branch)
		gc(tx, branchData.GetWorkspaceRoot()) // changes are destroyed here
		tx.DeleteBranch(branch)
		return nil, nil
	})
	return err
}

func (index *Index) Checkout(repo *model.Repo, branch, commit string) error {
	_, err := index.store.Transact(repo, func(tx Transaction) (interface{}, error) {
		tx.ClearWorkspace(branch)
		commitData, err := tx.ReadCommit(commit)
		if err != nil {
			return nil, err
		}
		branchData, err := tx.ReadBranch(branch)
		if err != nil {
			return nil, err
		}
		gc(tx, branchData.GetWorkspaceRoot())
		branchData.Commit = commit
		branchData.CommitRoot = commitData.GetTree()
		branchData.WorkspaceRoot = commitData.GetTree()
		err = tx.WriteBranch(branch, branchData)
		return nil, err
	})
	return err
}

func (index *Index) Merge(repo *model.Repo, source, destination string) error {
	_, err := index.store.Transact(repo, func(tx Transaction) (interface{}, error) {
		return nil, nil // TODO: optimistic concurrency based optimization
		// i.e. assume source branch receives no new commits since the start of the process
	})
	return err
}
