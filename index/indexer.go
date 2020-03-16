package index

import (
	"fmt"
	"math/rand"
	"time"

	log "github.com/sirupsen/logrus"

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
	ReadObject(repoId, ref, path string) (*model.Object, error)
	ReadEntryObject(repoId, ref, path string) (*model.Entry, error)
	ReadEntryTree(repoId, ref, path string) (*model.Entry, error)
	ReadRootObject(repoId, ref string) (*model.Root, error)
	WriteObject(repoId, branch, path string, object *model.Object) error
	WriteEntry(repoId, branch, path string, entry *model.Entry) error
	WriteFile(repoId, branch, path string, entry *model.Entry, obj *model.Object) error
	DeleteObject(repoId, branch, path string) error
	ListObjectsByPrefix(repoId, ref, path, after string, results int, descend bool) ([]*model.Entry, bool, error)
	ListBranchesByPrefix(repoId string, prefix string, amount int, after string) ([]*model.Branch, bool, error)
	ResetBranch(repoId, branch string) error
	CreateBranch(repoId, branch, ref string) (*model.Branch, error)
	GetBranch(repoId, branch string) (*model.Branch, error)
	Commit(repoId, branch, message, committer string, metadata map[string]string) (*model.Commit, error)
	GetCommit(repoId, commitId string) (*model.Commit, error)
	GetCommitLog(repoId, fromCommitId string) ([]*model.Commit, error)
	DeleteBranch(repoId, branch string) error
	Diff(repoId, leftRef, rightRef string) (merkle.Differences, error)
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
		err = partialCommit(tx, branch)
		if err != nil {
			return err
		}
	}
	return nil
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
	kv          store.Store
	tsGenerator TimeGenerator
}

type Option func(index *KVIndex)

type TimeGenerator func() int64

// Option to initiate with
// when using this option timestamps will generate using the given time generator
// used for mocking and testing timestamps
func WithTimeGenerator(generator TimeGenerator) Option {
	return func(kvi *KVIndex) {
		kvi.tsGenerator = generator
	}
}

func NewKVIndex(kv store.Store, opts ...Option) *KVIndex {
	kvi := &KVIndex{
		kv:          kv,
		tsGenerator: func() int64 { return time.Now().Unix() },
	}
	for _, opt := range opts {
		opt(kvi)
	}
	return kvi
}

type reference struct {
	commit   *model.Commit
	branch   *model.Branch
	isBranch bool
}

func (r *reference) String() string {
	if r.isBranch {
		return fmt.Sprintf("[branch='%s' -> commit='%s' -> root='%s']",
			r.branch.GetName(),
			r.commit.GetAddress(),
			r.commit.GetTree())
	}
	return fmt.Sprintf("[commit='%s' -> root='%s']",
		r.commit.GetAddress(),
		r.commit.GetTree())
}

func resolveRef(tx store.RepoReadOnlyOperations, ref string) (*reference, error) {
	// if this is not
	if ident.IsHash(ref) {
		// this looks like a straight up commit, let's see if it exists
		commit, err := tx.ReadCommit(ref)
		if err != nil && !xerrors.Is(err, db.ErrNotFound) {
			// got an error, we can't continue
			return nil, err
		} else if err == nil {
			// great, it's a commit, return it
			return &reference{
				commit: commit,
			}, nil
		}
	}
	// treat this as a branch name
	branch, err := tx.ReadBranch(ref)
	if err != nil {
		return nil, err
	}
	commit, err := tx.ReadCommit(branch.GetCommit())
	if err != nil {
		return nil, err
	}

	return &reference{
		commit:   commit,
		branch:   branch,
		isBranch: true,
	}, nil
}

// Business logic
func (index *KVIndex) ReadObject(repoId, ref, path string) (*model.Object, error) {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(ref),
		ValidatePath(path))
	if err != nil {
		return nil, err
	}

	obj, err := index.kv.RepoReadTransact(repoId, func(tx store.RepoReadOnlyOperations) (interface{}, error) {
		_, err := tx.ReadRepo()
		if err != nil {
			return nil, err
		}

		reference, err := resolveRef(tx, ref)
		if err != nil {
			return nil, err
		}
		var obj *model.Object

		if reference.isBranch {
			we, err := tx.ReadFromWorkspace(reference.branch.GetName(), path)
			if xerrors.Is(err, db.ErrNotFound) {
				// not in workspace, let's try reading it from branch tree
				m := merkle.New(reference.branch.GetWorkspaceRoot())
				obj, err = m.GetObject(tx, path)
				if err != nil {
					return nil, err
				}
				return obj, nil
			} else if err != nil {
				// an actual error has occurred, return it.
				return nil, err
			}
			if we.GetTombstone() {
				// object was deleted deleted
				return nil, db.ErrNotFound
			}
			return tx.ReadObject(we.GetEntry().GetAddress())
		}
		// otherwise, read from commit
		m := merkle.New(reference.commit.GetTree())
		obj, err = m.GetObject(tx, path)
		if err != nil {
			return nil, err
		}
		return obj, nil
	})
	if err != nil {
		return nil, err
	}
	return obj.(*model.Object), nil
}
func readEntry(tx store.RepoReadOnlyOperations, ref, path string, typ model.Entry_Type) (*model.Entry, error) {
	var entry *model.Entry

	_, err := tx.ReadRepo()
	if err != nil {
		return nil, err
	}

	reference, err := resolveRef(tx, ref)
	if err != nil {
		return nil, err
	}
	root := reference.commit.GetTree()
	if reference.isBranch {
		// try reading from workspace
		we, err := tx.ReadFromWorkspace(reference.branch.GetName(), path)

		// continue with we only if we got no error
		if err != nil {
			if !xerrors.Is(err, db.ErrNotFound) {
				return nil, err
			}
		} else {
			if we.GetTombstone() {
				// object was deleted deleted
				return nil, db.ErrNotFound
			}
			return we.GetEntry(), nil
		}
		root = reference.branch.GetWorkspaceRoot()
	}

	m := merkle.New(root)
	entry, err = m.GetEntry(tx, path, typ)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (index *KVIndex) ReadEntry(repoId, branch, path string, typ model.Entry_Type) (*model.Entry, error) {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch),
		ValidatePath(path))
	if err != nil {
		return nil, err
	}
	entry, err := index.kv.RepoReadTransact(repoId, func(tx store.RepoReadOnlyOperations) (interface{}, error) {
		return readEntry(tx, branch, path, typ)
	})
	if err != nil {
		return nil, err
	}
	return entry.(*model.Entry), nil
}

func (index *KVIndex) ReadRootObject(repoId, ref string) (*model.Root, error) {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(ref))
	if err != nil {
		return nil, err
	}
	root, err := index.kv.RepoReadTransact(repoId, func(tx store.RepoReadOnlyOperations) (i interface{}, err error) {
		_, err = tx.ReadRepo()
		if err != nil {
			return nil, err
		}
		reference, err := resolveRef(tx, ref)
		if err != nil {
			return nil, err
		}
		if reference.isBranch {
			return tx.ReadRoot(reference.branch.GetWorkspaceRoot())
		}
		return tx.ReadRoot(reference.commit.GetTree())
	})
	if err != nil {
		return nil, err
	}
	return root.(*model.Root), nil
}

func (index *KVIndex) ReadEntryTree(repoId, branch, path string) (*model.Entry, error) {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch),
		ValidatePath(path))
	if err != nil {
		return nil, err
	}
	return index.ReadEntry(repoId, branch, path, model.Entry_TREE)
}

func (index *KVIndex) ReadEntryObject(repoId, branch, path string) (*model.Entry, error) {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch),
		ValidatePath(path))
	if err != nil {
		return nil, err
	}
	return index.ReadEntry(repoId, branch, path, model.Entry_OBJECT)
}

func (index *KVIndex) WriteFile(repoId, branch, path string, entry *model.Entry, obj *model.Object) error {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch),
		ValidatePath(path))
	if err != nil {
		return err
	}
	_, err = index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
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
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch),
		ValidatePath(path))
	if err != nil {
		return err
	}
	_, err = index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
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
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch),
		ValidatePath(path))
	if err != nil {
		return err
	}
	timestamp := index.tsGenerator()
	_, err = index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
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
				Timestamp: timestamp,
				Size:      object.GetSize(),
				Checksum:  object.GetChecksum(),
			},
		})
		return nil, err
	})
	return err
}

// delete object with timestamp - for testing timestamps
func (index *KVIndex) DeleteObject(repoId, branch, path string) error {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch),
		ValidatePath(path))
	if err != nil {
		return err
	}
	ts := index.tsGenerator()
	_, err = index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
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
				Name:      pth.New(path).Basename(),
				Timestamp: ts,
				Type:      model.Entry_OBJECT,
			},
			Tombstone: true,
		})
		return nil, err
	})
	return err
}

func (index *KVIndex) ListBranchesByPrefix(repoId string, prefix string, amount int, after string) ([]*model.Branch, bool, error) {
	err := ValidateAll(
		ValidateRepoId(repoId))
	if err != nil {
		return nil, false, err
	}
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

func (index *KVIndex) ListObjectsByPrefix(repoId, ref, path, from string, results int, descend bool) ([]*model.Entry, bool, error) {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(ref),
		ValidatePath(path))
	if err != nil {
		return nil, false, err
	}
	type result struct {
		hasMore bool
		results []*model.Entry
	}
	entries, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		_, err := tx.ReadRepo()
		if err != nil {
			return nil, err
		}

		reference, err := resolveRef(tx, ref)
		if err != nil {
			return nil, err
		}

		var root string
		if reference.isBranch {
			err := partialCommit(tx, reference.branch.GetName()) // block on this since we traverse the tree immediately after
			if err != nil {
				return nil, err
			}
			root = reference.branch.GetWorkspaceRoot()
		} else {
			root = reference.commit.GetTree()
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
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch))
	if err != nil {
		return err
	}
	// clear workspace, set branch workspace root back to commit root
	_, err = index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
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

func (index *KVIndex) CreateBranch(repoId, branch, ref string) (*model.Branch, error) {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(ref),
		ValidateRef(branch))
	if err != nil {
		return nil, err
	}
	branchData, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		// ensure it doesn't exist yet
		_, err := tx.ReadBranch(branch)
		if err != nil && !xerrors.Is(err, db.ErrNotFound) {
			return nil, err
		} else if err == nil {
			return nil, errors.ErrBranchAlreadyExists
		}
		// read resolve reference
		reference, err := resolveRef(tx, ref)
		if err != nil {
			return nil, xerrors.Errorf("could not read ref: %w", err)
		}
		branchData := &model.Branch{
			Name:          branch,
			Commit:        reference.commit.GetAddress(),
			CommitRoot:    reference.commit.GetTree(),
			WorkspaceRoot: reference.commit.GetTree(),
		}
		return branchData, tx.WriteBranch(branch, branchData)
	})
	if err != nil {
		return nil, err
	}
	return branchData.(*model.Branch), nil
}

func (index *KVIndex) GetBranch(repoId, branch string) (*model.Branch, error) {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch))
	if err != nil {
		return nil, err
	}
	brn, err := index.kv.RepoReadTransact(repoId, func(tx store.RepoReadOnlyOperations) (i interface{}, err error) {
		return tx.ReadBranch(branch)
	})
	if err != nil {
		return nil, err
	}
	return brn.(*model.Branch), nil
}

func (index *KVIndex) Commit(repoId, branch, message, committer string, metadata map[string]string) (*model.Commit, error) {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch),
		ValidateCommitMessage(message))
	if err != nil {
		return nil, err
	}
	ts := index.tsGenerator()
	commit, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
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
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateCommitID(commitId))
	if err != nil {
		return nil, err
	}
	commit, err := index.kv.RepoReadTransact(repoId, func(tx store.RepoReadOnlyOperations) (i interface{}, err error) {
		return tx.ReadCommit(commitId)
	})
	if err != nil {
		return nil, err
	}
	return commit.(*model.Commit), nil
}

func (index *KVIndex) GetCommitLog(repoId, fromCommitId string) ([]*model.Commit, error) {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateCommitID(fromCommitId))
	if err != nil {
		return nil, err
	}
	commits, err := index.kv.RepoReadTransact(repoId, func(tx store.RepoReadOnlyOperations) (i interface{}, err error) {
		return dag.BfsScan(tx, fromCommitId)
	})
	if err != nil {
		return nil, err
	}
	return commits.([]*model.Commit), nil
}

func (index *KVIndex) DeleteBranch(repoId, branch string) error {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch))
	if err != nil {
		return err
	}
	_, err = index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
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
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch))
	if err != nil {
		return nil, err
	}
	res, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (i interface{}, err error) {
		err = partialCommit(tx, branch) // ensure all changes are reflected in the tree
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

func (index *KVIndex) Diff(repoId, leftRef, rightRef string) (merkle.Differences, error) {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(leftRef),
		ValidateRef(rightRef))
	if err != nil {
		return nil, err
	}
	res, err := index.kv.RepoReadTransact(repoId, func(tx store.RepoReadOnlyOperations) (i interface{}, err error) {

		lRef, err := resolveRef(tx, leftRef)
		if err != nil {
			log.WithError(err).WithField("ref", leftRef).Error("could not resolve left ref")
			return nil, err
		}

		rRef, err := resolveRef(tx, rightRef)
		if err != nil {
			log.WithError(err).WithField("ref", rRef).Error("could not resolve right ref")
			return nil, err
		}

		commonCommits, err := dag.FindLowestCommonAncestor(tx, lRef.commit.GetAddress(), rRef.commit.GetAddress())
		if err != nil {
			log.WithField("left", lRef).WithField("right", rRef).WithError(err).Error("could not find common commit")
			return nil, err
		}
		if len(commonCommits) == 0 {
			log.WithField("left", lRef).WithField("right", rRef).Error("no common merge base found")
			return nil, errors.ErrNoMergeBase
		}

		leftTree := lRef.commit.GetTree()
		if lRef.isBranch {
			leftTree = lRef.branch.GetWorkspaceRoot()
		}
		rightTree := rRef.commit.GetTree()

		diff, err := merkle.Diff(tx,
			merkle.New(leftTree),
			merkle.New(rightTree),
			merkle.New(commonCommits[0].GetTree()))
		if err != nil {
			log.WithField("left", lRef).WithField("right", rRef).WithError(err).Error("could not calculate diff")
		}
		return diff, err
	})
	if err != nil {
		return nil, err
	}
	return res.(merkle.Differences), nil
}

func (index *KVIndex) RevertCommit(repoId, branch, commit string) error {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch),
		ValidateCommitID(commit))
	if err != nil {
		return err
	}
	_, err = index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
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
	_, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		p := pth.New(path)
		if p.IsRoot() {
			return nil, index.ResetBranch(repoId, branch)
		}

		err := partialCommit(tx, branch)
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
		workspaceMerkle, err = workspaceMerkle.Update(tx, commitEntries)
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
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch),
		ValidatePath(path))
	if err != nil {
		return err
	}
	return index.revertPath(repoId, branch, path, model.Entry_TREE)
}

func (index *KVIndex) RevertObject(repoId, branch, path string) error {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch),
		ValidatePath(path))
	if err != nil {
		return err
	}
	return index.revertPath(repoId, branch, path, model.Entry_OBJECT)
}

func (index *KVIndex) Merge(repoId, source, destination string) error {
	_, err := index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		return nil, nil // TODO: optimistic concurrency based optimization
		// i.e. assume source branch receives no new commits since the start of the process
	})
	return err
}

func (index *KVIndex) CreateRepo(repoId, bucketName, defaultBranch string) error {
	err := ValidateAll(
		ValidateRepoId(repoId))
	if err != nil {
		return err
	}

	creationDate := index.tsGenerator()

	repo := &model.Repo{
		RepoId:             repoId,
		BucketName:         bucketName,
		CreationDate:       creationDate,
		DefaultBranch:      defaultBranch,
		PartialCommitRatio: DefaultPartialCommitRatio,
	}

	// create repository, an empty commit and tree, and the default branch
	_, err = index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
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
	err := ValidateAll(
		ValidateRepoId(repoId))
	if err != nil {
		return nil, err
	}
	repo, err := index.kv.ReadTransact(func(tx store.ClientReadOnlyOperations) (interface{}, error) {
		return tx.ReadRepo(repoId)
	})
	if err != nil {
		return nil, err
	}
	return repo.(*model.Repo), nil
}

func (index *KVIndex) DeleteRepo(repoId string) error {
	err := ValidateAll(
		ValidateRepoId(repoId))
	if err != nil {
		return err
	}
	_, err = index.kv.Transact(func(tx store.ClientOperations) (interface{}, error) {
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
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch))
	if err != nil {
		return err
	}
	_, err = index.kv.RepoTransact(repoId, func(tx store.RepoOperations) (interface{}, error) {
		err := partialCommit(tx, branch)
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
