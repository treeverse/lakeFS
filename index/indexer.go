package index

import (
	"math/rand"
	"time"
	"treeverse-lake/db"
	"treeverse-lake/ident"
	"treeverse-lake/index/merkle"
	"treeverse-lake/index/model"
	"treeverse-lake/index/store"

	"golang.org/x/xerrors"
)

const (
	// DefaultPartialCommitRatio is the ratio (1/?) of writes that will trigger a partial commit (number between 0-1)
	DefaultPartialCommitRatio = 0.005 // ~200 writes before a partial commit

	// DefaultBranch is the branch to be automatically created when a repo is born
	DefaultBranch = "master"
)

type Index interface {
	ReadObject(clientId, repoId, branch, path string) (*model.Object, error)
	WriteObject(clientId, repoId, branch, path string, object *model.Object) error
	DeleteObject(clientId, repoId, branch, path string) error
	ListObjects(clientId, repoId, branch, path, from string, results int) ([]*model.Entry, bool, error)
	ListBranches(clientId, repoId string, results int) ([]*model.Entry, error)
	ResetBranch(clientId, repoId, branch string) error
	Commit(clientId, repoId, branch, message, committer string, metadata map[string]string) error
	DeleteBranch(clientId, repoId, branch string) error
	Checkout(clientId, repoId, branch, commit string) error
	Merge(clientId, repoId, source, destination string) error
	CreateRepo(clientId, repoId, defaultBranch string) error
	ListRepos(clientId string) ([]*model.Repo, error)
	GetRepo(clientId, repoId string) (*model.Repo, error)
	DeleteRepo(clientId, repoId string) error
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
	if xerrors.Is(err, db.ErrNotFound) {
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
	if xerrors.Is(err, db.ErrNotFound) {
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
		Name:          branch,
		Commit:        branchData.GetCommit(),
		CommitRoot:    branchData.GetCommitRoot(),
		WorkspaceRoot: tree.Root(), // does this happen properly?
	})
	if err != nil {
		return err
	}

	// done!
	return nil
}

func gc(tx store.RepoOperations, addr string) {
	// TODO: impl? here?
}

type KVIndex struct {
	kv store.Store
}

func NewKVIndex(kv store.Store) *KVIndex {
	return &KVIndex{kv: kv}
}

// Business logic
func (index *KVIndex) ReadObject(clientId, repoId, branch, path string) (*model.Object, error) {
	obj, err := index.kv.RepoReadTransact(clientId, repoId, func(tx store.RepoReadOnlyOperations) (interface{}, error) {
		var obj *model.Object
		we, err := tx.ReadFromWorkspace(branch, path)
		if xerrors.Is(err, db.ErrNotFound) {
			// not in workspace, let's try reading it from branch tree
			repo, err := tx.ReadRepo()
			if err != nil {
				return nil, err
			}
			root, err := resolveReadRoot(tx, repo, branch)
			if err != nil {
				return nil, err
			}
			m := merkle.New(root)
			obj, err = m.GetObject(tx, path)
			if err != nil {
				return nil, err
			}
			return obj, nil
		}
		if err != nil {
			// an actual error has occurred, return it.
			return nil, err
		}
		if we.GetTombstone() != nil {
			// object was deleted deleted
			return nil, db.ErrNotFound
		}

		return we.GetObject(), nil
	})
	if err != nil {
		return nil, err
	}
	return obj.(*model.Object), nil
}

func (index *KVIndex) WriteObject(clientId, repoId, branch, path string, object *model.Object) error {
	_, err := index.kv.RepoTransact(clientId, repoId, func(tx store.RepoOperations) (interface{}, error) {
		addr := ident.Hash(object)
		err := tx.WriteObject(addr, object)
		if err != nil {
			return nil, err
		}
		repo, err := tx.ReadRepo()
		if err != nil {
			return nil, err
		}
		//Data: &model.WorkspaceEntry_Address{Address: addr},
		err = writeEntryToWorkspace(tx, repo, branch, path, &model.WorkspaceEntry{
			Path: path,
			Data: &model.WorkspaceEntry_Object{Object: object},
		})
		return nil, err
	})
	return err
}

func (index *KVIndex) DeleteObject(clientId, repoId, branch, path string) error {
	_, err := index.kv.RepoTransact(clientId, repoId, func(tx store.RepoOperations) (interface{}, error) {
		repo, err := tx.ReadRepo()
		if err != nil {
			return nil, err
		}
		err = writeEntryToWorkspace(tx, repo, branch, path, &model.WorkspaceEntry{
			Data: &model.WorkspaceEntry_Tombstone{Tombstone: &model.Tombstone{}},
		})
		return nil, err
	})
	return err
}

func (index *KVIndex) ListBranches(clientId, repoId string, results int) ([]*model.Entry, error) {
	entries, err := index.kv.RepoTransact(clientId, repoId, func(tx store.RepoOperations) (interface{}, error) {
		_, err := tx.ReadRepo()
		if err != nil {
			return nil, err
		}
		branches, err := tx.ListBranches()
		if err != nil {
			return nil, err
		}
		entries := make([]*model.Entry, len(branches))
		for i, branch := range branches {
			entries[i] = &model.Entry{
				Name:    branch.GetName(),
				Address: branch.GetName(),
				Type:    model.Entry_TREE,
			}
		}
		return entries, nil
	})
	if err != nil {
		return nil, err
	}
	return entries.([]*model.Entry), nil
}

func (index *KVIndex) ListObjects(clientId, repoId, branch, path, from string, results int) ([]*model.Entry, bool, error) {
	type result struct {
		hasMore bool
		results []*model.Entry
	}
	entries, err := index.kv.RepoTransact(clientId, repoId, func(tx store.RepoOperations) (interface{}, error) {
		err := partialCommit(tx, branch) // block on this since we traverse the tree immediately after
		if err != nil {
			return nil, err
		}
		repo, err := tx.ReadRepo()
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
		res, hasMore, err := tx.ListTree(addr, from, results)
		if err != nil {
			return nil, err
		}
		return &result{hasMore, res}, nil
	})
	if err != nil {
		return nil, false, err
	}
	return entries.(*result).results, entries.(*result).hasMore, nil
}

func (index *KVIndex) ResetBranch(clientId, repoId, branch string) error {
	// clear workspace, set branch workspace root back to commit root
	_, err := index.kv.RepoTransact(clientId, repoId, func(tx store.RepoOperations) (interface{}, error) {
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

func (index *KVIndex) Commit(clientId, repoId, branch, message, committer string, metadata map[string]string) error {
	ts := time.Now().Unix()
	_, err := index.kv.RepoTransact(clientId, repoId, func(tx store.RepoOperations) (interface{}, error) {
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

func (index *KVIndex) DeleteBranch(clientId, repoId, branch string) error {
	_, err := index.kv.RepoTransact(clientId, repoId, func(tx store.RepoOperations) (interface{}, error) {
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

func (index *KVIndex) Checkout(clientId, repoId, branch, commit string) error {
	_, err := index.kv.RepoTransact(clientId, repoId, func(tx store.RepoOperations) (interface{}, error) {
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

func (index *KVIndex) Merge(clientId, repoId, source, destination string) error {
	_, err := index.kv.RepoTransact(clientId, repoId, func(tx store.RepoOperations) (interface{}, error) {
		return nil, nil // TODO: optimistic concurrency based optimization
		// i.e. assume source branch receives no new commits since the start of the process
	})
	return err
}

func (index *KVIndex) CreateRepo(clientId, repoId, defaultBranch string) error {

	creationDate := time.Now().Unix()

	repo := &model.Repo{
		ClientId:           clientId,
		RepoId:             repoId,
		CreationDate:       creationDate,
		DefaultBranch:      defaultBranch,
		PartialCommitRatio: DefaultPartialCommitRatio,
	}

	// create repository, an empty commit and tree, and the default branch
	_, err := index.kv.RepoTransact(clientId, repoId, func(tx store.RepoOperations) (interface{}, error) {
		err := tx.WriteRepo(repo)
		if err != nil {
			return nil, err
		}
		commit := &model.Commit{
			Tree:      ident.Empty(),
			Parents:   []string{},
			Message:   "Repository Epoch",
			Timestamp: creationDate,
			Metadata:  make(map[string]string),
		}
		commitId := ident.Hash(commit)
		err = tx.WriteCommit(commitId, commit)
		if err != nil {
			return nil, err
		}
		err = tx.WriteBranch(repo.GetDefaultBranch(), &model.Branch{
			Name:          repo.GetDefaultBranch(),
			Commit:        commitId,
			CommitRoot:    commit.GetTree(),
			WorkspaceRoot: commit.GetTree(),
		})
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

func (index *KVIndex) GetRepo(clientId, repoId string) (*model.Repo, error) {
	repo, err := index.kv.ClientReadTransact(clientId, func(tx store.ClientReadOnlyOperations) (interface{}, error) {
		return tx.ReadRepo(repoId)
	})
	if err != nil {
		return nil, err
	}
	return repo.(*model.Repo), nil
}

func (index *KVIndex) DeleteRepo(clientId, repoId string) error {
	_, err := index.kv.ClientTransact(clientId, func(tx store.ClientOperations) (interface{}, error) {
		tx.DeleteRepo(repoId)
		return nil, nil
	})
	return err
}
