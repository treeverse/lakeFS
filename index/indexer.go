package index

import (
	"math/rand"
	"regexp"
	"time"

	"github.com/treeverse/lakefs/index/dag"

	"github.com/treeverse/lakefs/index/errors"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/index/merkle"
	"github.com/treeverse/lakefs/index/model"
	pth "github.com/treeverse/lakefs/index/path"
	"github.com/treeverse/lakefs/index/store"

	"golang.org/x/xerrors"
)

const (
	// DefaultPartialCommitRatio is the ratio (1/?) of writes that will trigger a partial commit (number between 0-1)
	DefaultPartialCommitRatio = 1 // 1 writes before a partial commit

	// DefaultBranch is the branch to be automatically created when a repo is born
	DefaultBranch = "master"
)

type Index interface {
	Tree(repoId, branch string) error
	ReadObject(repoId, branch, path string) (*model.Object, error)
	ReadEntryObject(repoId, branch, path string) (*model.Entry, error)
	ReadRootObject(repoId, branch string) (*model.Root, error)
	WriteObject(repoId, branch, path string, object *model.Object) error
	WriteEntry(repoId, branch, path string, entry *model.Entry) error
	WriteFile(repoId, branch, path string, entry *model.Entry, obj *model.Object) error
	DeleteObject(repoId, branch, path string) error
	ListObjectsByPrefix(repoId, branch, path, after string, results int, descend bool) ([]*model.Entry, bool, error)
	ListBranchesByPrefix(repoId string, prefix string, amount int, after string) ([]*model.Branch, bool, error)
	ResetBranch(repoId, branch string) error
	CreateBranch(repoId, branch, commitId string) error
	GetBranch(repoId, branch string) (*model.Branch, error)
	Commit(repoId, branch, message, committer string, metadata map[string]string) (*model.Commit, error)
	GetCommit(repoId, commitId string) (*model.Commit, error)
	GetCommitLog(repoId, fromCommitId string) ([]*model.Commit, error)
	DeleteBranch(repoId, branch string) error
	Diff(repoId, branch, otherBranch string) (merkle.Differences, error)
	DiffWorkspace(repoId, branch string) (merkle.Differences, error)
	RevertCommit(repoId, branch, commit string) error
	RevertPath(repoId, branch, path string) error
	RevertObject(repoId, branch, path string) error
	Merge(repoId, source, destination string) error
	CreateRepo(repoId, bucketName, defaultBranch string) error
	ListRepos(amount int, after string) ([]*model.Repo, bool, error)
	GetRepo(repoId string) (*model.Repo, error)
	DeleteRepo(repoId string) error
}

func writeEntryToWorkspace(tx store.RepoOperations, repo *model.Repo, branch, path string, entry *model.WorkspaceEntry) error {
	err := tx.WriteToWorkspacePath(branch, path, entry)
	if err != nil {
		return err
	}
	if shouldPartiallyCommit(repo) {
		err = partialCommit(tx, branch, entry.GetEntry().GetTimestamp())
		if err != nil {
			return err
		}
	}
	return nil
}

func resolveReadRoot(tx store.RepoReadOnlyOperations, branch string) (string, error) {
	var empty string
	branchData, err := tx.ReadBranch(branch)
	if err != nil {
		return empty, err // unexpected error
	}
	return branchData.GetWorkspaceRoot(), nil
}

func shouldPartiallyCommit(repo *model.Repo) bool {
	chosen := rand.Float32()
	return chosen < repo.GetPartialCommitRatio()
}

func partialCommit(tx store.RepoOperations, branch string, ts int64) error {
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
	tree, err = tree.Update(tx, wsEntries, ts)
	if err != nil {
		return err
	}

	// clear workspace entries
	err = tx.ClearWorkspace(branch)
	if err != nil {
		return err
	}

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
func (index *KVIndex) ReadObject(repoId, branch, path string) (*model.Object, error) {
	obj, err := index.kv.RepoReadTransact(repoId, func(tx store.RepoReadOnlyOperations) (interface{}, error) {
		var obj *model.Object
		we, err := tx.ReadFromWorkspace(branch, path)
		if xerrors.Is(err, db.ErrNotFound) {
			// not in workspace, let's try reading it from branch tree
			_, err := tx.ReadRepo()
			if err != nil {
				return nil, err
			}
			root, err := resolveReadRoot(tx, branch)
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
		if we.GetTombstone() {
			// object was deleted deleted
			return nil, db.ErrNotFound
		}
		return tx.ReadObject(we.GetEntry().GetAddress())
	})
	if err != nil {
		return nil, err
	}
	return obj.(*model.Object), nil
}
func readEntry(tx store.RepoReadOnlyOperations, branch, path string, typ model.Entry_Type) (*model.Entry, error) {
	var entry *model.Entry
	we, err := tx.ReadFromWorkspace(branch, path)
	if xerrors.Is(err, db.ErrNotFound) {
		// not in workspace, let's try reading it from branch tree
		_, err := tx.ReadRepo()
		if err != nil {
			return nil, err
		}
		root, err := resolveReadRoot(tx, branch)
		if err != nil {
			return nil, err
		}
		m := merkle.New(root)
		entry, err = m.GetEntry(tx, path, typ)
		if err != nil {
			return nil, err
		}
		return entry, nil
	}
	if err != nil {
		// an actual error has occurred, return it.
		return nil, err
	}
	if we.GetTombstone() {
		// object was deleted deleted
		return nil, db.ErrNotFound
	}
	// exists in workspace
	return we.GetEntry(), nil
}
func (index *KVIndex) ReadEntry(repoId, branch, path string, typ model.Entry_Type) (*model.Entry, error) {
	entry, err := index.kv.RepoReadTransact(repoId, func(tx store.RepoReadOnlyOperations) (interface{}, error) {
		return readEntry(tx, branch, path, typ)
	})
	if err != nil {
		return nil, err
	}
	return entry.(*model.Entry), nil
}
func (index *KVIndex) ReadRootObject(repoId, branch string) (*model.Root, error) {
	root, err := index.kv.RepoReadTransact(repoId, func(tx store.RepoReadOnlyOperations) (i interface{}, err error) {
		_, err = tx.ReadRepo()
		if err != nil {
			return nil, err
		}
		root, err := resolveReadRoot(tx, branch)
		if err != nil {
			return nil, err
		}
		return tx.ReadRoot(root)
	})
	if err != nil {
		return nil, err
	}
	return root.(*model.Root), nil
}
func (index *KVIndex) ReadEntryTree(repoId, branch, path string) (*model.Entry, error) {
	return index.ReadEntry(repoId, branch, path, model.Entry_TREE)
}

func (index *KVIndex) ReadEntryObject(repoId, branch, path string) (*model.Entry, error) {
	return index.ReadEntry(repoId, branch, path, model.Entry_OBJECT)
}

func (index *KVIndex) WriteFile(repoId, branch, path string, entry *model.Entry, obj *model.Object) error {
	_, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		repo, err := tx.ReadRepo()
		if err != nil {
			return nil, err
		}
		err = tx.WriteObject(ident.Hash(obj), obj)
		if err != nil {
			return nil, err
		}
		err = writeEntryToWorkspace(tx, repo, branch, path, &model.WorkspaceEntry{
			Path:  path,
			Entry: entry,
		})
		return nil, err
	})
	return err
}

func (index *KVIndex) WriteEntry(repoId, branch, path string, entry *model.Entry) error {
	_, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		repo, err := tx.ReadRepo()
		if err != nil {
			return nil, err
		}
		err = writeEntryToWorkspace(tx, repo, branch, path, &model.WorkspaceEntry{
			Path:  path,
			Entry: entry,
		})
		return nil, err
	})
	return err
}

func (index *KVIndex) WriteObject(repoId, branch, path string, object *model.Object) error {
	timestamp := time.Now()
	_, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		addr := ident.Hash(object)
		err := tx.WriteObject(addr, object)
		if err != nil {
			return nil, err
		}
		repo, err := tx.ReadRepo()
		if err != nil {
			return nil, err
		}
		p := pth.New(path)
		err = writeEntryToWorkspace(tx, repo, branch, path, &model.WorkspaceEntry{
			Path: p.String(),
			Entry: &model.Entry{
				Name:      pth.New(path).Basename(),
				Address:   addr,
				Type:      model.Entry_OBJECT,
				Timestamp: timestamp.Unix(),
				Size:      object.GetSize(),
				Checksum:  object.GetChecksum(),
			},
		})
		return nil, err
	})
	return err
}

func (index *KVIndex) DeleteObject(repoId, branch, path string) error {
	_, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		repo, err := tx.ReadRepo()
		if err != nil {
			return nil, err
		}

		_, err = readEntry(tx, branch, path, model.Entry_OBJECT)
		if err != nil {
			return nil, err
		}
		err = writeEntryToWorkspace(tx, repo, branch, path, &model.WorkspaceEntry{
			Path: path,
			Entry: &model.Entry{
				Name: pth.New(path).Basename(),
				Type: model.Entry_OBJECT,
			},
			Tombstone: true,
		})
		return nil, err
	})
	return err
}

func (index *KVIndex) ListBranchesByPrefix(repoId string, prefix string, amount int, after string) ([]*model.Branch, bool, error) {
	type result struct {
		hasMore bool
		results []*model.Branch
	}

	entries, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		// we're reading the repo to add it to this transaction's conflict range
		// but also to ensure it exists
		_, err := tx.ReadRepo()
		if err != nil {
			return nil, err
		}
		branches, hasMore, err := tx.ListBranches(prefix, amount, after)
		return &result{
			results: branches,
			hasMore: hasMore,
		}, err
	})
	if err != nil {
		return nil, false, err
	}
	return entries.(*result).results, entries.(*result).hasMore, nil
}

func (index *KVIndex) ListObjectsByPrefix(repoId, branch, path, from string, results int, descend bool) ([]*model.Entry, bool, error) {
	type result struct {
		hasMore bool
		results []*model.Entry
	}
	ts := time.Now().Unix()
	entries, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		err := partialCommit(tx, branch, ts) // block on this since we traverse the tree immediately after
		if err != nil {
			return nil, err
		}
		_, err = tx.ReadRepo()
		if err != nil {
			return nil, err
		}
		root, err := resolveReadRoot(tx, branch)
		if err != nil {
			return nil, err
		}
		tree := merkle.New(root)
		res, hasMore, err := tree.PrefixScan(tx, path, from, results, descend)
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

func (index *KVIndex) ResetBranch(repoId, branch string) error {
	// clear workspace, set branch workspace root back to commit root
	_, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		err := tx.ClearWorkspace(branch)
		if err != nil {
			return nil, err
		}
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

func (index *KVIndex) CreateBranch(repoId, branch, commitId string) error {
	_, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		// ensure it doesn't exist yet
		_, err := tx.ReadBranch(branch)
		if err != nil && !xerrors.Is(err, db.ErrNotFound) {
			return nil, err
		} else if err == nil {
			return nil, errors.ErrBranchExists
		}
		// read commit at commitId
		commit, err := tx.ReadCommit(commitId)
		if err != nil {
			return nil, xerrors.Errorf("could not read commit: %w", err)
		}
		return nil, tx.WriteBranch(branch, &model.Branch{
			Name:          branch,
			Commit:        commitId,
			CommitRoot:    commit.GetTree(),
			WorkspaceRoot: commit.GetTree(),
		})
	})
	return err
}

func (index *KVIndex) GetBranch(repoId, branch string) (*model.Branch, error) {
	brn, err := index.kv.RepoReadTransact(repoId, func(tx store.RepoReadOnlyOperations) (i interface{}, err error) {
		return tx.ReadBranch(branch)
	})
	if err != nil {
		return nil, err
	}
	return brn.(*model.Branch), nil
}

func (index *KVIndex) Commit(repoId, branch, message, committer string, metadata map[string]string) (*model.Commit, error) {
	ts := time.Now().Unix()
	commit, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		err := partialCommit(tx, branch, ts)
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
		commit.Address = commitAddr
		err = tx.WriteCommit(commitAddr, commit)
		if err != nil {
			return nil, err
		}
		branchData.Commit = commitAddr
		branchData.CommitRoot = commit.GetTree()
		branchData.WorkspaceRoot = commit.GetTree()

		return commit, tx.WriteBranch(branch, branchData)
	})
	if err != nil {
		return nil, err
	}
	return commit.(*model.Commit), nil
}

func (index *KVIndex) GetCommit(repoId, commitId string) (*model.Commit, error) {
	commit, err := index.kv.RepoReadTransact(repoId, func(tx store.RepoReadOnlyOperations) (i interface{}, err error) {
		return tx.ReadCommit(commitId)
	})
	if err != nil {
		return nil, err
	}
	return commit.(*model.Commit), nil
}

func (index *KVIndex) GetCommitLog(repoId, fromCommitId string) ([]*model.Commit, error) {
	commits, err := index.kv.RepoReadTransact(repoId, func(tx store.RepoReadOnlyOperations) (i interface{}, err error) {
		return dag.BfsScan(tx, fromCommitId)
	})
	if err != nil {
		return nil, err
	}
	return commits.([]*model.Commit), nil
}

func (index *KVIndex) DeleteBranch(repoId, branch string) error {
	_, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		branchData, err := tx.ReadBranch(branch)
		if err != nil {
			return nil, err
		}
		err = tx.ClearWorkspace(branch)
		if err != nil {
			return nil, err
		}
		gc(tx, branchData.GetWorkspaceRoot()) // changes are destroyed here
		err = tx.DeleteBranch(branch)
		return nil, err
	})
	return err
}

func (index *KVIndex) DiffWorkspace(repoId, branch string) (merkle.Differences, error) {
	ts := time.Now().Unix()
	res, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (i interface{}, err error) {
		err = partialCommit(tx, branch, ts) // ensure all changes are reflected in the tree
		if err != nil {
			return nil, err
		}
		branch, err := tx.ReadBranch(branch)
		if err != nil {
			return nil, err
		}

		diff, err := merkle.Diff(tx,
			merkle.New(branch.GetWorkspaceRoot()),
			merkle.New(branch.GetCommitRoot()),
			merkle.New(branch.GetCommitRoot()))
		return diff, err
	})
	if err != nil {
		return nil, err
	}
	return res.(merkle.Differences), nil
}

func (index *KVIndex) Diff(repoId, branch, otherBranch string) (merkle.Differences, error) {
	res, err := index.kv.RepoReadTransact(repoId, func(tx store.RepoReadOnlyOperations) (i interface{}, err error) {

		leftBranch, err := tx.ReadBranch(branch)
		if err != nil {
			return nil, err
		}

		rightBranch, err := tx.ReadBranch(otherBranch)
		if err != nil {
			return nil, err
		}

		commonCommits, err := dag.FindLowestCommonAncestor(tx, leftBranch.GetCommit(), rightBranch.GetCommit())
		if err != nil {
			return nil, err
		}
		if len(commonCommits) == 0 {
			return nil, errors.ErrNoMergeBase
		}

		diff, err := merkle.Diff(tx,
			merkle.New(leftBranch.GetCommitRoot()),
			merkle.New(rightBranch.GetCommitRoot()),
			merkle.New(commonCommits[0].GetTree()))
		return diff, err
	})
	if err != nil {
		return nil, err
	}
	return res.(merkle.Differences), nil
}

func (index *KVIndex) RevertCommit(repoId, branch, commit string) error {
	_, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		err := tx.ClearWorkspace(branch)
		if err != nil {
			return nil, err
		}
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

func (index *KVIndex) revertPath(repoId, branch, path string, typ model.Entry_Type) error {
	ts := time.Now().Unix()
	_, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		p := pth.New(path)
		if p.IsRoot() {
			return nil, index.ResetBranch(repoId, branch)
		}

		err := partialCommit(tx, branch, ts)
		if err != nil {
			return nil, err
		}
		branchData, err := tx.ReadBranch(branch)
		if err != nil {
			return nil, err
		}
		workspaceMerkle := merkle.New(branchData.GetWorkspaceRoot())
		commitMerkle := merkle.New(branchData.GetCommitRoot())
		var workspaceEntry *model.WorkspaceEntry
		commitEntry, err := commitMerkle.GetEntry(tx, path, typ)
		if err != nil {
			if xerrors.Is(err, db.ErrNotFound) {
				// remove all changes under path
				pathEntry, err := workspaceMerkle.GetEntry(tx, path, typ)
				if err != nil {
					return nil, err
				}
				workspaceEntry = &model.WorkspaceEntry{
					Path:      path,
					Entry:     pathEntry,
					Tombstone: true,
				}
			} else {
				return nil, err
			}
		} else {
			workspaceEntry = &model.WorkspaceEntry{
				Path:  path,
				Entry: commitEntry,
			}
		}
		commitEntries := []*model.WorkspaceEntry{workspaceEntry}
		workspaceMerkle, err = workspaceMerkle.Update(tx, commitEntries, ts)
		if err != nil {
			return nil, err
		}

		// update branch workspace pointer to point at new workspace
		err = tx.WriteBranch(branch, &model.Branch{
			Name:          branch,
			Commit:        branchData.GetCommit(),
			CommitRoot:    branchData.GetCommitRoot(),
			WorkspaceRoot: workspaceMerkle.Root(),
		})

		return nil, err
	})
	return err
}

func (index *KVIndex) RevertPath(repoId, branch, path string) error {
	return index.revertPath(repoId, branch, path, model.Entry_TREE)
}

func (index *KVIndex) RevertObject(repoId, branch, path string) error {
	return index.revertPath(repoId, branch, path, model.Entry_OBJECT)
}

func (index *KVIndex) Merge(repoId, source, destination string) error {
	_, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		return nil, nil // TODO: optimistic concurrency based optimization
		// i.e. assume source branch receives no new commits since the start of the process
	})
	return err
}

func isValidRepoId(repoId string) bool {
	return regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,62}$`).MatchString(repoId)
}

func (index *KVIndex) CreateRepo(repoId, bucketName, defaultBranch string) error {

	if !isValidRepoId(repoId) {
		return errors.ErrInvalidBucketName
	}

	creationDate := time.Now().Unix()

	repo := &model.Repo{
		RepoId:             repoId,
		BucketName:         bucketName,
		CreationDate:       creationDate,
		DefaultBranch:      defaultBranch,
		PartialCommitRatio: DefaultPartialCommitRatio,
	}

	// create repository, an empty commit and tree, and the default branch
	_, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		// make sure this repo doesn't already exist
		_, err := tx.ReadRepo()
		if err == nil {
			// couldn't verify this bucket doesn't yet exist
			return nil, errors.ErrRepoExists
		} else if !xerrors.Is(err, db.ErrNotFound) {
			return nil, err // error reading the repo
		}

		err = tx.WriteRepo(repo)
		if err != nil {
			return nil, err
		}
		commit := &model.Commit{
			Tree:      ident.Empty(),
			Parents:   []string{},
			Timestamp: creationDate,
			Metadata:  make(map[string]string),
		}
		commitId := ident.Hash(commit)
		commit.Address = commitId
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

func (index *KVIndex) ListRepos(amount int, after string) ([]*model.Repo, bool, error) {
	type result struct {
		repos   []*model.Repo
		hasMore bool
	}
	res, err := index.kv.ReadTransact(func(tx store.ClientReadOnlyOperations) (interface{}, error) {
		repos, hasMore, err := tx.ListRepos(amount, after)
		return &result{
			repos:   repos,
			hasMore: hasMore,
		}, err
	})
	if err != nil {
		return nil, false, err
	}
	return res.(*result).repos, res.(*result).hasMore, nil
}

func (index *KVIndex) GetRepo(repoId string) (*model.Repo, error) {
	repo, err := index.kv.ReadTransact(func(tx store.ClientReadOnlyOperations) (interface{}, error) {
		return tx.ReadRepo(repoId)
	})
	if err != nil {
		return nil, err
	}
	return repo.(*model.Repo), nil
}

func (index *KVIndex) DeleteRepo(repoId string) error {
	_, err := index.kv.Transact(func(tx store.ClientOperations) (interface{}, error) {
		_, err := tx.ReadRepo(repoId)
		if err != nil {
			return nil, err
		}
		err = tx.DeleteRepo(repoId)
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}

func (index *KVIndex) Tree(repoId, branch string) error {
	ts := time.Now().Unix()
	_, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		err := partialCommit(tx, branch, ts)
		if err != nil {
			return nil, err
		}
		_, err = tx.ReadRepo()
		if err != nil {
			return nil, err
		}
		r, err := tx.ReadBranch(branch)
		if err != nil {
			return nil, err
		}
		m := merkle.New(r.GetWorkspaceRoot())
		m.WalkAll(tx)
		return nil, nil
	})
	return err
}
