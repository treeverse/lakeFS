package index

import (
	"math/rand"
	"time"
	"versio-index/ident"
	"versio-index/index/errors"
	"versio-index/index/merkle"
	"versio-index/index/model"
	"versio-index/index/store"

	"golang.org/x/xerrors"
)

// DefaultPartialCommitRatio is the ratio of writes that will trigger a partial commit (number between 0-1)
const DefaultPartialCommitRatio = 0.02 // ~50 writes before a partial commit

type Index interface {
	Read(repo *model.Repo, branch, path string) (*model.Object, error)
	Write(repo *model.Repo, branch, path string, object *model.Object) error
	Delete(repo *model.Repo, branch, path string) error
	List(repo *model.Repo, branch, path string) ([]*model.Entry, error)
	Reset(repo *model.Repo, branch string) error
	Commit(repo *model.Repo, branch, message, committer string, metadata map[string]string) error
	DeleteBranch(repo *model.Repo, branch string) error
	Checkout(repo *model.Repo, branch, commit string) error
	Merge(repo *model.Repo, source, destination string) error
	CreateRepo(clientId, repoId, defaultBranch string) error
}

func writeEntryToWorkspace(tx store.RepoOperations, repo *model.Repo, branch, path string, entry *model.WorkspaceEntry) error {
	err := tx.WriteToWorkspacePath(branch, path, entry)
	if err != nil {
		return err
	}
	if shouldPartiallyCommit(repo) {
		err = partialCommit(tx, branch)
		if err != nil {
			return err
		}
	}
	return nil
}

func resolveReadRoot(tx store.RepoReadOnlyOperations, repo *model.Repo, branch string) (string, error) {
	var empty string
	branchData, err := tx.ReadBranch(branch)
	if xerrors.Is(err, errors.ErrNotFound) {
		// fallback to default branch
		branchData, err = tx.ReadBranch(repo.DefaultBranch)
		if err != nil {
			return empty, err
		}
		return branchData.GetCommitRoot(), nil // when falling back we don't want the dirty writes
	} else if err != nil {
		return empty, err // unexpected error
	}
	return branchData.GetWorkspaceRoot(), nil
}

func shouldPartiallyCommit(repo *model.Repo) bool {
	chosen := rand.Float32()
	return chosen < repo.GetPartialCommitRatio()
}

type KVIndex struct {
	kv store.Store
}

func NewKVIndex(kv store.Store) *KVIndex {
	return &KVIndex{kv: kv}
}

// Business logic
func (index *KVIndex) Read(repo *model.Repo, branch, path string) (*model.Object, error) {
	obj, err := index.kv.RepoReadTransact(repo, func(tx store.RepoReadOnlyOperations) (interface{}, error) {
		var obj *model.Object
		we, err := tx.ReadFromWorkspace(branch, path)
		if err != nil && !xerrors.Is(err, errors.ErrNotFound) {
			// an actual error has occurred, return it.
			return nil, err
		}
		if we.GetTombstone() != nil {
			// object was deleted deleted
			return nil, errors.ErrNotFound
		}
		if xerrors.Is(err, errors.ErrNotFound) {
			// not in workspace, let's try reading it from branch tree
			root, err := resolveReadRoot(tx, repo, branch)
			if err != nil {
				return nil, err
			}
			m := merkle.New(root)
			obj, err = m.GetObject(tx, path)
			if err != nil {
				return nil, err
			}
		}
		return obj, nil
	})
	if err != nil {
		return nil, err
	}
	return obj.(*model.Object), nil
}

func (index *KVIndex) Write(repo *model.Repo, branch, path string, object *model.Object) error {
	_, err := index.kv.RepoTransact(repo, func(tx store.RepoOperations) (interface{}, error) {
		addr := ident.Hash(object)
		err := tx.WriteObject(addr, object)
		if err != nil {
			return nil, err
		}
		err = writeEntryToWorkspace(tx, repo, branch, path, &model.WorkspaceEntry{
			Path: path,
			Data: &model.WorkspaceEntry_Address{Address: addr},
		})
		return nil, err
	})
	return err
}

func (index *KVIndex) Delete(repo *model.Repo, branch, path string) error {
	_, err := index.kv.RepoTransact(repo, func(tx store.RepoOperations) (interface{}, error) {
		err := writeEntryToWorkspace(tx, repo, branch, path, &model.WorkspaceEntry{
			Data: &model.WorkspaceEntry_Tombstone{Tombstone: &model.Tombstone{}},
		})
		return nil, err
	})
	return err
}

func partialCommit(tx store.RepoOperations, branch string) error {
	// see if we have any changes that weren't applied
	wsEntries, err := tx.ListWorkspace(branch)
	if err != nil {
		return err
	}
	if len(wsEntries) == 0 {
		return nil
	}

	// get branch info (including current workspace root)
	branchData, err := tx.ReadBranch(branch)
	if xerrors.Is(err, errors.ErrNotFound) {
		return nil
	} else if err != nil {
		return err // unexpected error
	}

	// update the immutable Merkle tree, getting back a new tree
	tree := merkle.New(branchData.GetWorkspaceRoot())
	tree, err = tree.Update(tx, wsEntries)
	if err != nil {
		return err
	}

	// clear workspace entries
	tx.ClearWorkspace(branch)

	// update branch pointer to point at new workspace
	err = tx.WriteBranch(branch, &model.Branch{
		Commit:        branchData.GetCommit(),
		CommitRoot:    branchData.GetCommitRoot(),
		WorkspaceRoot: tree.Root(),
	})
	if err != nil {
		return err
	}

	// done!
	return nil
}

func (index *KVIndex) List(repo *model.Repo, branch, path string) ([]*model.Entry, error) {
	entries, err := index.kv.RepoTransact(repo, func(tx store.RepoOperations) (interface{}, error) {
		err := partialCommit(tx, branch)
		if err != nil {
			return nil, err
		}

		root, err := resolveReadRoot(tx, repo, branch)
		if err != nil {
			return nil, err
		}
		tree := merkle.New(root)
		addr, err := tree.GetAddress(tx, path, model.Entry_TREE)
		if err != nil {
			return nil, err
		}
		return tx.ListTree(addr) // TODO: enrich this list with object metadata
	})
	if err != nil {
		return nil, err
	}
	return entries.([]*model.Entry), nil
}

func gc(tx store.RepoOperations, addr string) {
	// TODO: impl? here?
}

func (index *KVIndex) Reset(repo *model.Repo, branch string) error {
	// clear workspace, set branch workspace root back to commit root
	_, err := index.kv.RepoTransact(repo, func(tx store.RepoOperations) (interface{}, error) {
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

func (index *KVIndex) Commit(repo *model.Repo, branch, message, committer string, metadata map[string]string) error {
	ts := time.Now().Unix()
	_, err := index.kv.RepoTransact(repo, func(tx store.RepoOperations) (interface{}, error) {
		err := partialCommit(tx, branch)
		if err != nil {
			return nil, err
		}
		branchData, err := tx.ReadBranch(branch)
		if err != nil {
			return nil, err
		}
		commit := &model.Commit{
			Tree:      branchData.GetWorkspaceRoot(),
			Parents:   []string{branchData.GetCommit()},
			Committer: committer,
			Message:   message,
			Timestamp: ts,
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

func (index *KVIndex) DeleteBranch(repo *model.Repo, branch string) error {
	_, err := index.kv.RepoTransact(repo, func(tx store.RepoOperations) (interface{}, error) {
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

func (index *KVIndex) Checkout(repo *model.Repo, branch, commit string) error {
	_, err := index.kv.RepoTransact(repo, func(tx store.RepoOperations) (interface{}, error) {
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

func (index *KVIndex) Merge(repo *model.Repo, source, destination string) error {
	_, err := index.kv.RepoTransact(repo, func(tx store.RepoOperations) (interface{}, error) {
		return nil, nil // TODO: optimistic concurrency based optimization
		// i.e. assume source branch receives no new commits since the start of the process
	})
	return err
}

func (index *KVIndex) CreateRepo(clientId, repoId, defaultBranch string) error {
	repo := &model.Repo{
		ClientId:           clientId,
		RepoId:             repoId,
		DefaultBranch:      defaultBranch,
		PartialCommitRatio: DefaultPartialCommitRatio,
	}
	_, err := index.kv.RepoTransact(repo, func(tx store.RepoOperations) (interface{}, error) {
		err := tx.WriteRepo(repo)
		return nil, err
	})
	return err
}

func (index *KVIndex) ListRepos(clientId string) ([]*model.Repo, error) {
	repos, err := index.kv.ClientReadTransact(clientId, func(tx store.ClientReadOnlyOperations) (interface{}, error) {
		return tx.ListRepos()
	})
	if err != nil {
		return nil, err
	}
	return repos.([]*model.Repo), nil
}
