package catalog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/logging"
)

const (
	// DefaultBranch is the branch to be automatically created when a repo is born
	DefaultBranch = "master"
)

type Indexer interface {
	WithContext(ctx context.Context) Indexer
	ReadObject(repoId, ref, path string, readUncommitted bool) (*Object, error)
	ReadEntryObject(repoId, ref, path string, readUncommitted bool) (*Entry, error)
	WriteObject(repoId, branch, path string, object *Object) error
	WriteEntry(repoId, branch, path string, entry *Entry) error
	WriteFile(repoId, branch, path string, entry *Entry, obj *Object) error
	DeleteObject(repoId, branch, path string) error
	ListObjectsByPrefix(repoId, ref, path, after string, results int, descend, readUncommitted bool) ([]*Entry, bool, error)
	ListBranchesByPrefix(repoId string, prefix string, amount int, after string) ([]*Branch, bool, error)
	ResetBranch(repoId, branch string) error
	CreateBranch(repoId, branch, ref string) (*Branch, error)
	GetBranch(repoId, branch string) (*Branch, error)
	Commit(repoId, branch, message, committer string, metadata map[string]string) (*Commit, error)
	GetCommit(repoId, commitId string) (*Commit, error)
	GetCommitLog(repoId, fromCommitId string, results int, after string) ([]*Commit, bool, error)
	DeleteBranch(repoId, branch string) error
	Diff(repoId, leftRef, rightRef string) (Differences, error)
	DiffWorkspace(repoId, branch string) (Differences, error)
	RevertCommit(repoId, branch, commit string) error
	RevertPath(repoId, branch, path string) error
	RevertObject(repoId, branch, path string) error
	Merge(repoId, source, destination, userId string) (Differences, error)
	CreateRepo(repoId, bucketName, defaultBranch string) error
	ListRepos(amount int, after string) ([]*Repo, bool, error)
	GetRepo(repoId string) (*Repo, error)
	DeleteRepo(repoId string) error
	CreateDedupEntryIfNone(repoId string, dedupId string, objName string) (string, error)
	CreateMultiPartUpload(repoId, id, path, objectName string, creationDate time.Time) error
	ReadMultiPartUpload(repoId, uploadId string) (*MultipartUpload, error)
	DeleteMultiPartUpload(repoId, uploadId string) error
}

func (index *DBIndexer) writeEntryToWorkspace(tx RepoOperations, branch, path string, entry *WorkspaceEntry) error {
	err := tx.WriteToWorkspacePath(branch, entry.ParentPath, path, entry)
	if err != nil {
		return err
	}
	return nil
}

type DBIndexer struct {
	store       Store
	tsGenerator TimeGenerator
	ctx         context.Context
}

type Option func(index *DBIndexer)

type TimeGenerator func() time.Time

// Option to initiate with
// when using this option timestamps will generate using the given time generator
// used for mocking and testing timestamps
func WithTimeGenerator(generator TimeGenerator) Option {
	return func(dbi *DBIndexer) {
		dbi.tsGenerator = generator
	}
}

func NewDBIndex(db db.Database, opts ...Option) *DBIndexer {
	kvi := &DBIndexer{
		store:       NewDBStore(db),
		tsGenerator: time.Now,
		ctx:         context.Background(),
	}
	for _, opt := range opts {
		opt(kvi)
	}
	kvi.log().Info("initialized Metadata index")
	return kvi
}

func indexLogger(ctx context.Context) logging.Logger {
	return logging.FromContext(ctx).WithField("service_name", "index")
}

func (index *DBIndexer) log() logging.Logger {
	return indexLogger(index.ctx)
}

// Business logic
func (index *DBIndexer) WithContext(ctx context.Context) Indexer {
	return &DBIndexer{
		store:       WithLogger(index.store, indexLogger(ctx)),
		tsGenerator: index.tsGenerator,
		ctx:         ctx,
	}
}

type reference struct {
	commit   *Commit
	branch   *Branch
	isBranch bool
}

func (r *reference) String() string {
	if r.isBranch {
		return fmt.Sprintf("[branch='%s' -> commit='%s' -> root='%s']",
			r.branch.Id,
			r.commit.Address,
			r.commit.Tree)
	}
	return fmt.Sprintf("[commit='%s' -> root='%s']",
		r.commit.Address,
		r.commit.Tree)
}

func resolveRef(tx RepoOperations, ref string) (*reference, error) {
	// if this is not
	if ident.IsHash(ref) {
		// this looks like a straight up commit, let's see if it exists
		commit, err := tx.ReadCommit(ref)
		if err != nil && !errors.Is(err, db.ErrNotFound) {
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
	commit, err := tx.ReadCommit(branch.CommitId)
	if err != nil {
		return nil, err
	}

	return &reference{
		commit:   commit,
		branch:   branch,
		isBranch: true,
	}, nil
}

func (index *DBIndexer) ReadObject(repoId, ref, path string, readUncommitted bool) (*Object, error) {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(ref),
		ValidatePath(path),
	)
	if err != nil {
		return nil, err
	}

	obj, err := index.store.RepoTransact(repoId, func(tx RepoOperations) (interface{}, error) {
		_, err := tx.ReadRepo()
		if err != nil {
			return nil, err
		}

		reference, err := resolveRef(tx, ref)
		if err != nil {
			return nil, err
		}
		var obj *Object

		if reference.isBranch && readUncommitted {
			we, err := tx.ReadFromWorkspace(reference.branch.Id, path)
			if err == nil && !we.Tombstone {
				return tx.ReadObject(*we.EntryAddress)
			} else if err == nil && we.Tombstone {
				// object was deleted
				return nil, db.ErrNotFound
			} else if !errors.Is(err, db.ErrNotFound) {
				// an actual error has occurred, return it.
				index.log().WithError(err).Error("could not read from workspace")
				return nil, err
			}
			// entry not found in workspace, read from commit
		}
		m := NewMerkle(reference.commit.Tree, MerkleWithLogger(index.log()))
		obj, err = m.GetObject(tx, path)
		if err != nil {
			return nil, err
		}
		return obj, nil
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	return obj.(*Object), nil
}

func (index *DBIndexer) readEntry(tx RepoOperations, ref, path, typ string, readUncommitted bool) (*Entry, error) {
	var entry *Entry

	_, err := tx.ReadRepo()
	if err != nil {
		return nil, err
	}

	reference, err := resolveRef(tx, ref)
	if err != nil {
		return nil, err
	}
	root := reference.commit.Tree
	if reference.isBranch && readUncommitted {
		// try reading from workspace
		we, err := tx.ReadFromWorkspace(reference.branch.Id, path)

		// continue with we only if we got no error
		if err != nil {
			if !errors.Is(err, db.ErrNotFound) {
				return nil, err
			}
		} else {
			if we.Tombstone {
				// object was deleted
				return nil, db.ErrNotFound
			}
			return &Entry{
				RepositoryId: we.RepositoryId,
				Name:         *we.EntryName,
				Address:      *we.EntryAddress,
				EntryType:    *we.EntryType,
				CreationDate: *we.EntryCreationDate,
				Size:         *we.EntrySize,
				Checksum:     *we.EntryChecksum,
			}, nil
		}
	}

	m := NewMerkle(root, MerkleWithLogger(index.log()))
	entry, err = m.GetEntry(tx, path, typ)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (index *DBIndexer) ReadEntry(repoId, branch, path, typ string, readUncommitted bool) (*Entry, error) {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch),
		ValidatePath(path),
	)
	if err != nil {
		return nil, err
	}
	entry, err := index.store.RepoTransact(repoId, func(tx RepoOperations) (interface{}, error) {
		return index.readEntry(tx, branch, path, typ, readUncommitted)
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	return entry.(*Entry), nil
}

func (index *DBIndexer) ReadEntryObject(repoId, ref, path string, readUncommitted bool) (*Entry, error) {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(ref),
		ValidatePath(path),
	)
	if err != nil {
		return nil, err
	}
	return index.ReadEntry(repoId, ref, path, EntryTypeObject, readUncommitted)
}

func (index *DBIndexer) WriteFile(repoId, branch, path string, entry *Entry, obj *Object) error {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch),
		ValidatePath(path),
	)
	if err != nil {
		return err
	}
	_, err = index.store.RepoTransact(repoId, func(tx RepoOperations) (interface{}, error) {
		err = tx.WriteObject(ident.Hash(obj), obj)
		if err != nil {
			index.log().WithError(err).Error("could not write object")
			return nil, err
		}
		err = index.writeEntryToWorkspace(tx, branch, path, &WorkspaceEntry{
			RepositoryId:      repoId,
			BranchId:          branch,
			ParentPath:        NewPath(path, entry.EntryType).ParentPath(),
			Path:              path,
			EntryName:         &entry.Name,
			EntryAddress:      &entry.Address,
			EntryType:         &entry.EntryType,
			EntryCreationDate: &entry.CreationDate,
			EntrySize:         &entry.Size,
			EntryChecksum:     &entry.Checksum,
			Tombstone:         false,
		})
		if err != nil {
			index.log().WithError(err).Error("could not write workspace entry")
		}
		return nil, err
	})
	return err
}

func (index *DBIndexer) WriteEntry(repoId, branch, path string, entry *Entry) error {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch),
		ValidatePath(path))
	if err != nil {
		return err
	}
	_, err = index.store.RepoTransact(repoId, func(tx RepoOperations) (interface{}, error) {
		err = index.writeEntryToWorkspace(tx, branch, path, &WorkspaceEntry{
			RepositoryId:      repoId,
			BranchId:          branch,
			ParentPath:        NewPath(path, entry.EntryType).ParentPath(),
			Path:              path,
			EntryName:         &entry.Name,
			EntryAddress:      &entry.Address,
			EntryType:         &entry.EntryType,
			EntryCreationDate: &entry.CreationDate,
			EntrySize:         &entry.Size,
			EntryChecksum:     &entry.Checksum,
			Tombstone:         false,
		})
		if err != nil {
			index.log().WithError(err).Error("could not write workspace entry")
		}
		return nil, err
	})
	return err
}

func (index *DBIndexer) WriteObject(repoId, branch, path string, object *Object) error {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch),
		ValidatePath(path))
	if err != nil {
		return err
	}
	timestamp := index.tsGenerator()
	_, err = index.store.RepoTransact(repoId, func(tx RepoOperations) (interface{}, error) {
		addr := ident.Hash(object)
		err := tx.WriteObject(addr, object)
		if err != nil {
			return nil, err
		}
		typ := EntryTypeObject
		p := NewPath(path, typ)
		entryName := NewPath(path, typ).BaseName()
		err = index.writeEntryToWorkspace(tx, branch, path, &WorkspaceEntry{
			RepositoryId:      repoId,
			Path:              p.String(),
			ParentPath:        p.ParentPath(),
			BranchId:          branch,
			EntryName:         &entryName,
			EntryAddress:      &addr,
			EntryType:         &typ,
			EntryCreationDate: &timestamp,
			EntrySize:         &object.Size,
			EntryChecksum:     &object.Checksum,
			Tombstone:         false,
		})
		if err != nil {
			index.log().WithError(err).Error("could not write workspace entry")
		}
		return nil, err
	})
	return err
}

func (index *DBIndexer) DeleteObject(repoId, branch, path string) error {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch),
		ValidatePath(path))
	if err != nil {
		return err
	}
	ts := index.tsGenerator()
	_, err = index.store.RepoTransact(repoId, func(tx RepoOperations) (interface{}, error) {
		/**
		handling 5 possible cases:
		* 1 object does not exist  - return error
		* 2 object exists only in workspace - remove from workspace
		* 3 object exists only in merkle - add tombstone
		* 4 object exists in workspace and in merkle - 2 + 3
		* 5 objects exists in merkle tombstone exists in workspace - return error
		*/
		notFoundCount := 0
		wsEntry, err := tx.ReadFromWorkspace(branch, path)
		if err != nil {
			if errors.Is(err, db.ErrNotFound) {
				notFoundCount += 1
			} else {
				return nil, err
			}
		}

		br, err := tx.ReadBranch(branch)
		if err != nil {
			return nil, err
		}
		root := br.CommitRoot
		m := NewMerkle(root, MerkleWithLogger(index.log()))
		merkleEntry, err := m.GetEntry(tx, path, EntryTypeObject)
		if err != nil {
			if errors.Is(err, db.ErrNotFound) {
				notFoundCount += 1
			} else {
				return nil, err
			}
		}

		if notFoundCount == 2 {
			return nil, db.ErrNotFound
		}

		if wsEntry != nil {
			if wsEntry.Tombstone {
				return nil, db.ErrNotFound
			}
			err = tx.DeleteWorkspacePath(branch, path, EntryTypeObject)
			if err != nil {
				return nil, err
			}
		}

		if merkleEntry != nil {
			typ := EntryTypeObject
			pathObj := NewPath(path, typ)
			bname := pathObj.BaseName()
			err = index.writeEntryToWorkspace(tx, branch, path, &WorkspaceEntry{
				Path:              path,
				EntryName:         &bname,
				EntryCreationDate: &ts,
				EntryType:         &typ,
				ParentPath:        pathObj.ParentPath(),
				Tombstone:         true,
			})
			if err != nil {
				index.log().WithError(err).Error("could not write workspace tombstone")
			}
			return nil, err
		}
		return nil, nil
	})
	return err
}

func (index *DBIndexer) ListBranchesByPrefix(repoId string, prefix string, amount int, after string) ([]*Branch, bool, error) {
	err := ValidateAll(
		ValidateRepoId(repoId))
	if err != nil {
		return nil, false, err
	}
	type result struct {
		hasMore bool
		results []*Branch
	}

	entries, err := index.store.RepoTransact(repoId, func(tx RepoOperations) (interface{}, error) {
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
		index.log().WithError(err).Error("could not list branches")
		return nil, false, err
	}
	return entries.(*result).results, entries.(*result).hasMore, nil
}

func (index *DBIndexer) ListObjectsByPrefix(repoId, ref, path, from string, results int, descend, readUncommitted bool) ([]*Entry, bool, error) {
	log := index.log().WithFields(logging.Fields{
		"from":    from,
		"descend": descend,
		"results": results,
	})
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(ref),
		ValidatePath(path),
	)
	if err != nil {
		return nil, false, err
	}
	type result struct {
		hasMore bool
		results []*Entry
	}
	entries, err := index.store.RepoTransact(repoId, func(tx RepoOperations) (interface{}, error) {
		_, err := tx.ReadRepo()
		if err != nil {
			return nil, err
		}

		reference, err := resolveRef(tx, ref)
		if err != nil {
			return nil, err
		}

		tree := NewMerkle(reference.commit.Tree, MerkleWithLogger(index.log()))
		var res, currentRes []*Entry
		var wsHasMore bool
		treeHasMore := true
		amountForQueries := -1
		if results > -1 {
			amountForQueries = results + 1
		}
		path = NewPath(path, EntryTypeObject).String()
		for (len(res) < amountForQueries || results < 0) && (treeHasMore || wsHasMore) {
			currentRes, treeHasMore, err = tree.PrefixScan(tx, path, from, amountForQueries, descend)
			if err != nil && !errors.Is(err, db.ErrNotFound) {
				log.WithError(err).Error("could not scan tree")
				return nil, err
			}
			var wsEntries []*WorkspaceEntry
			if reference.isBranch && readUncommitted {
				if descend {
					wsEntries, wsHasMore, err = tx.ListWorkspaceWithPrefix(reference.branch.Id, path, from, amountForQueries)
					if err != nil {
						log.WithError(err).Error("failed to list workspace")
						return nil, err
					}
				} else {
					pathObj := NewPath(path, EntryTypeObject)
					wsEntries, wsHasMore, err = tx.ListWorkspaceDirectory(reference.branch.Id, pathObj.ParentPath(), pathObj.BaseName(), from, amountForQueries)
					if err != nil {
						log.WithError(err).Error("failed to list workspace")
						return nil, err
					}
				}
				currentRes, from = CombineLists(currentRes, wsEntries, treeHasMore, wsHasMore, amountForQueries-len(res))
			}
			res = append(res, currentRes...)
		}
		hasMore := false
		if results >= 0 && len(res) > results {
			hasMore = true
			res = res[:results]
		}
		return &result{hasMore, res}, nil
	})
	if err != nil {
		return nil, false, err
	}
	return entries.(*result).results, entries.(*result).hasMore, nil
}
func combineCompareEntries(entries []*Entry, treeIndex int, treeHasMore bool, wsEntries []*WorkspaceEntry, wsIndex int, wsHasMore bool) (int, bool) {
	var treeEntry *Entry
	var wsEntry *WorkspaceEntry
	if treeIndex < len(entries) {
		treeEntry = entries[treeIndex]
	}
	if wsIndex < len(wsEntries) {
		wsEntry = wsEntries[wsIndex]
	}
	if wsEntry != nil && treeEntry != nil {
		return strings.Compare(treeEntry.Name, wsEntry.Path), true
	}
	if !wsHasMore && treeEntry != nil {
		return -1, true
	}
	if !treeHasMore && wsEntry != nil {
		return 1, true
	}
	return 0, false
}

// CombineLists takes a sorted array of WorkspaceEntry and sorted a array of Entry, and combines them into one sorted array, omitting deleted files.
// The returned array will be no longer than the given amount.
func CombineLists(entries []*Entry, wsEntries []*WorkspaceEntry, treeHasMore bool, wsHasMore bool, amount int) ([]*Entry, string) {
	var result []*Entry
	var treeIndex, wsIndex int
	var newFrom string
Loop:
	for treeIndex < len(entries) || wsIndex < len(wsEntries) {
		if amount > 0 && len(result) == amount {
			break
		}
		switch compareResult, shouldContinue := combineCompareEntries(entries, treeIndex, treeHasMore, wsEntries, wsIndex, wsHasMore); {
		case !shouldContinue:
			break Loop
		case compareResult < 0:
			result = append(result, entries[treeIndex])
			newFrom = entries[treeIndex].Name
			treeIndex++
		case compareResult == 0:
			treeIndex++
			fallthrough
		case compareResult > 0:
			if (compareResult > 0 || entries[treeIndex-1].ObjectCount-wsEntries[wsIndex].TombstoneCount > 0) && !wsEntries[wsIndex].Tombstone {
				result = append(result, wsEntries[wsIndex].EntryWithPathAsName())
			}
			newFrom = wsEntries[wsIndex].Path
			wsIndex++
		}
	}
	return result, newFrom
}

func (index *DBIndexer) ResetBranch(repoId, branch string) error {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch))
	if err != nil {
		return err
	}
	// clear workspace, set branch workspace root back to commit root
	_, err = index.store.RepoTransact(repoId, func(tx RepoOperations) (interface{}, error) {
		return nil, tx.ClearWorkspace(branch)
	})
	if err != nil {
		index.log().WithError(err).Error("could not reset branch")
	}
	return err
}

func (index *DBIndexer) CreateBranch(repoId, branch, ref string) (*Branch, error) {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(ref),
		ValidateRef(branch))
	if err != nil {
		return nil, err
	}
	branchData, err := index.store.RepoTransact(repoId, func(tx RepoOperations) (interface{}, error) {
		// ensure it doesn't exist yet
		_, err := tx.ReadBranch(branch)
		if err != nil && !errors.Is(err, db.ErrNotFound) {
			index.log().WithError(err).Error("could not read branch")
			return nil, err
		}
		if err == nil {
			return nil, ErrBranchAlreadyExists
		}
		// read resolve reference
		reference, err := resolveRef(tx, ref)
		if err != nil {
			return nil, fmt.Errorf("could not read ref: %w", err)
		}
		branchData := &Branch{
			Id:         branch,
			CommitId:   reference.commit.Address,
			CommitRoot: reference.commit.Tree,
		}
		return branchData, tx.WriteBranch(branch, branchData)
	})
	if err != nil {
		index.log().WithError(err).WithField("ref", ref).Error("could not create branch")
		return nil, err
	}
	return branchData.(*Branch), nil
}

func (index *DBIndexer) GetBranch(repoId, branch string) (*Branch, error) {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch))
	if err != nil {
		return nil, err
	}
	brn, err := index.store.RepoTransact(repoId, func(tx RepoOperations) (i interface{}, err error) {
		return tx.ReadBranch(branch)
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	return brn.(*Branch), nil
}

func doCommitUpdates(tx RepoOperations, branchData *Branch, committer, message string, parents []string, metadata map[string]string, ts time.Time, index *DBIndexer) (*Commit, error) {
	commit := &Commit{
		Tree:         branchData.CommitRoot,
		Parents:      parents,
		Committer:    committer,
		Message:      message,
		CreationDate: ts,
		Metadata:     metadata,
	}
	commitAddr := ident.Hash(commit)
	commit.Address = commitAddr
	err := tx.WriteCommit(commitAddr, commit)
	if err != nil {
		index.log().WithError(err).Error("could not write commit")
		return nil, err
	}
	branchData.CommitId = commitAddr
	branchData.CommitRoot = commit.Tree

	return commit, tx.WriteBranch(branchData.Id, branchData)
}

func (index *DBIndexer) Commit(repoId, branch, message, committer string, metadata map[string]string) (*Commit, error) {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch),
		ValidateCommitMessage(message))
	if err != nil {
		return nil, err
	}
	ts := index.tsGenerator()
	commit, err := index.store.RepoTransact(repoId, func(tx RepoOperations) (interface{}, error) {
		// see if we have any changes that weren't applied
		err := tx.LockWorkspace()
		if err != nil {
			return nil, err
		}
		wsEntries, err := tx.ListWorkspace(branch)
		if err != nil {
			return nil, err
		}
		branchData, err := tx.ReadBranch(branch)
		if err != nil {
			return nil, err // unexpected error
		}

		// update the immutable Merkle tree, getting back a new tree
		tree := NewMerkle(branchData.CommitRoot, MerkleWithLogger(index.log()))
		tree, err = tree.Update(tx, wsEntries)
		if err != nil {
			return nil, err
		}
		// clear workspace entries
		err = tx.ClearWorkspace(branch)
		if err != nil {
			return nil, err
		}
		branchData.CommitRoot = tree.Root()
		return doCommitUpdates(tx, branchData, committer, message, []string{branchData.CommitId}, metadata, ts, index)

	})
	if err != nil {
		return nil, err
	}
	return commit.(*Commit), nil
}

func (index *DBIndexer) GetCommit(repoId, commitId string) (*Commit, error) {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateCommitID(commitId))
	if err != nil {
		return nil, err
	}
	commit, err := index.store.RepoTransact(repoId, func(tx RepoOperations) (interface{}, error) {
		return tx.ReadCommit(commitId)
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	return commit.(*Commit), nil
}

func (index *DBIndexer) GetCommitLog(repoId, fromCommitId string, results int, after string) ([]*Commit, bool, error) {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateCommitID(fromCommitId),
		ValidateOrEmpty(ValidateCommitID, after))

	type result struct {
		hasMore bool
		results []*Commit
	}
	if err != nil {
		return nil, false, err
	}
	res, err := index.store.RepoTransact(repoId, func(tx RepoOperations) (i interface{}, err error) {
		commits, hasMore, err := CommitScan(tx, fromCommitId, results, after)
		return &result{hasMore, commits}, err
	}, db.ReadOnly())
	if err != nil {
		index.log().WithError(err).WithField("from", fromCommitId).Error("could not read commits")
		return nil, false, err
	}
	return res.(*result).results, res.(*result).hasMore, nil
}

func (index *DBIndexer) DeleteBranch(repoId, branch string) error {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch))
	if err != nil {
		return err
	}
	_, err = index.store.RepoTransact(repoId, func(tx RepoOperations) (interface{}, error) {
		_, err := tx.ReadBranch(branch)
		if err != nil {
			return nil, err
		}
		err = tx.ClearWorkspace(branch)
		if err != nil {
			index.log().WithError(err).Error("could not clear workspace")
			return nil, err
		}
		err = tx.DeleteBranch(branch)
		if err != nil {
			index.log().WithError(err).Error("could not delete branch")
		}
		return nil, err
	})
	return err
}

func doDiff(tx RepoOperations, leftRef, rightRef string, index *DBIndexer) (Differences, error) {
	lRef, err := resolveRef(tx, leftRef)
	if err != nil {
		index.log().WithError(err).WithField("ref", leftRef).Error("could not resolve left ref")
		return nil, ErrBranchNotFound
	}

	rRef, err := resolveRef(tx, rightRef)
	if err != nil {
		index.log().WithError(err).WithField("ref", rRef).Error("could not resolve right ref")
		return nil, ErrBranchNotFound
	}

	commonCommits, err := FindLowestCommonAncestor(tx, lRef.commit.Address, rRef.commit.Address)
	if err != nil {
		index.log().WithField("left", lRef).WithField("right", rRef).WithError(err).Error("could not find common commit")
		return nil, ErrNoMergeBase
	}
	if commonCommits == nil {
		index.log().WithField("left", lRef).WithField("right", rRef).Error("no common merge base found")
		return nil, ErrNoMergeBase
	}

	leftTree := lRef.commit.Tree
	rightTree := rRef.commit.Tree

	diff, err := Diff(tx,
		NewMerkle(leftTree, MerkleWithLogger(index.log())),
		NewMerkle(rightTree, MerkleWithLogger(index.log())),
		NewMerkle(commonCommits.Tree, MerkleWithLogger(index.log())),
	)
	if err != nil {
		index.log().WithField("left", lRef).WithField("right", rRef).WithError(err).Error("could not calculate diff")
	}
	return diff, err
}

func (index *DBIndexer) Diff(repoId, leftRef, rightRef string) (Differences, error) {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(leftRef),
		ValidateRef(rightRef),
	)
	if err != nil {
		return nil, err
	}
	res, err := index.store.RepoTransact(repoId, func(tx RepoOperations) (i interface{}, err error) {
		return doDiff(tx, leftRef, rightRef, index)
	})

	return res.(Differences), nil
}

func (index *DBIndexer) RevertCommit(repoId, branch, commit string) error {
	log := index.log().WithFields(logging.Fields{
		"branch": branch,
		"commit": commit,
	})
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch),
		ValidateCommitID(commit))
	if err != nil {
		return err
	}
	_, err = index.store.RepoTransact(repoId, func(tx RepoOperations) (interface{}, error) {
		err := tx.ClearWorkspace(branch)
		if err != nil {
			log.WithError(err).Error("could not revert commit")
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
		branchData.CommitId = commit
		branchData.CommitRoot = commitData.Tree
		err = tx.WriteBranch(branch, branchData)
		if err != nil {
			log.WithError(err).Error("could not write branch")
		}
		return nil, err
	})
	return err
}

func (index *DBIndexer) revertPath(repoId, branch, path, typ string) error {
	log := index.log().WithFields(logging.Fields{
		"branch": branch,
		"path":   path,
	})
	_, err := index.store.RepoTransact(repoId, func(tx RepoOperations) (interface{}, error) {
		err := tx.DeleteWorkspacePath(branch, path, typ)
		if err != nil {
			log.WithError(err).Error("could not revert path")
		}
		return nil, err
	})
	return err
}

func (index *DBIndexer) RevertPath(repoId, branch, path string) error {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch),
		ValidatePath(path))
	if err != nil {
		return err
	}
	return index.revertPath(repoId, branch, path, EntryTypeTree)
}

func (index *DBIndexer) RevertObject(repoId, branch, path string) error {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(branch),
		ValidatePath(path))
	if err != nil {
		return err
	}
	return index.revertPath(repoId, branch, path, EntryTypeObject)
}

func (index *DBIndexer) Merge(repoId, source, destination, userId string) (Differences, error) {
	err := ValidateAll(
		ValidateRepoId(repoId),
		ValidateRef(source),
		ValidateRef(destination),
	)
	if err != nil {
		return nil, err
	}
	ts := index.tsGenerator()
	var mergeOperations Differences
	_, err = index.store.RepoTransact(repoId, func(tx RepoOperations) (interface{}, error) {
		// check that destination has no uncommitted changes
		destinationBranch, err := tx.ReadBranch(destination)
		if err != nil {
			index.log().WithError(err).WithField("destination", destination).Warn(" branch " + destination + " not found")
			return nil, ErrBranchNotFound
		}
		_, hasMore, err := tx.ListWorkspaceWithPrefix(destination, "", "", 0) // check if there are uncommitted changes
		if err != nil {
			index.log().WithError(err).WithField("destination", destination).Warn(" branch " + destination + " workspace not found")
			return nil, err
		}
		if hasMore {
			return nil, ErrDestinationNotCommitted
		}
		// compute difference
		df, err := doDiff(tx, source, destination, index)
		if err != nil {
			return nil, err
		}
		var isConflict bool
		for _, dif := range df {
			if dif.Direction == DifferenceDirectionConflict {
				isConflict = true
			}
			if dif.Direction != DifferenceDirectionRight {
				mergeOperations = append(mergeOperations, dif)
			}
		}
		if isConflict {
			return nil, ErrMergeConflict
		}
		// update destination with source changes
		var wsEntries []*WorkspaceEntry
		sourceBranch, err := tx.ReadBranch(source)
		if err != nil {
			index.log().WithError(err).Fatal("failed reading source branch") // failure to read a branch that was read before fatal
			return nil, err
		}
		for _, dif := range mergeOperations {
			var e *Entry
			m := NewMerkle(sourceBranch.CommitRoot, MerkleWithLogger(index.log()))
			if dif.Type != DifferenceTypeRemoved {
				e, err = m.GetEntry(tx, dif.Path, dif.PathType)
				if err != nil {
					index.log().WithError(err).Fatal("failed reading entry")
					return nil, err
				}
			} else {
				e = new(Entry)
				p := strings.Split(dif.Path, "/")
				e.Name = p[len(p)-1]
				e.EntryType = dif.PathType
			}
			w := new(WorkspaceEntry)
			w.EntryType = &e.EntryType
			w.EntryAddress = &e.Address
			w.EntryName = &e.Name
			w.EntryChecksum = &e.Checksum
			w.EntryCreationDate = &e.CreationDate
			w.EntrySize = &e.Size
			w.Path = dif.Path
			w.Tombstone = dif.Type == DifferenceTypeRemoved
			wsEntries = append(wsEntries, w)
		}

		destinationRoot := NewMerkle(destinationBranch.CommitRoot, MerkleWithLogger(index.log()))
		newRoot, err := destinationRoot.Update(tx, wsEntries)
		if err != nil {
			index.log().WithError(err).Fatal("failed updating merge destination")
			return nil, ErrMergeUpdateFailed
		}
		destinationBranch.CommitRoot = newRoot.Root()

		// read commits for each branch in our merge.
		// check which parents is older by searching the other parents using our DAG
		// 1. read commits
		// 2. use iterator with the first commit to lookup the other commit
		branches := []*Branch{sourceBranch, destinationBranch}
		commits := make([]*Commit, len(branches))
		for i, branch := range branches {
			var err error
			commits[i], err = tx.ReadCommit(branch.CommitId)
			if err != nil {
				index.log().WithError(err).Error("failed read commit")
				return nil, fmt.Errorf("missing commit: %w", err)
			}
		}
		parent1Commit := commits[0]
		parent2Commit := commits[1]
		iter := NewCommitIterator(tx, parent2Commit.Address)
		parent2Older := true
		for iter.Next() {
			if iter.Value().Address == parent1Commit.Address {
				parent2Older = false
				break
			}
		}
		if iter.Err() != nil {
			index.log().WithError(err).Error("failed while lookup parent relation")
			return nil, fmt.Errorf("failed to scan parent commits: %w", err)
		}
		if !parent2Older {
			commits[0], commits[1] = commits[1], commits[0]
		}
		parents := []string{commits[0].Address, commits[1].Address}

		commitMessage := "Merge branch " + source + " into " + destination
		_, err = doCommitUpdates(tx, destinationBranch, userId, commitMessage, parents, make(map[string]string), ts, index)
		if err != nil {
			index.log().WithError(err).WithFields(logging.Fields{
				"source":      source,
				"destination": destination,
				"userId":      userId,
				"parents":     parents,
			}).Error("commit merge branch")
			return nil, fmt.Errorf("failed to commit updates: %w", err)
		}
		return mergeOperations, nil
	})
	// ErrMergeConflict is the only error that will report the merge operations made so far
	if err != nil && err != ErrMergeConflict {
		return nil, err
	}
	return mergeOperations, err
}

func (index *DBIndexer) CreateRepo(repoId, bucketName, defaultBranch string) error {
	err := ValidateAll(
		ValidateRepoId(repoId))
	if err != nil {
		return err
	}

	creationDate := index.tsGenerator()

	repo := &Repo{
		Id:               repoId,
		StorageNamespace: bucketName,
		CreationDate:     creationDate,
		DefaultBranch:    defaultBranch,
	}

	// create repository, an empty commit and tree, and the default branch
	_, err = index.store.RepoTransact(repoId, func(tx RepoOperations) (interface{}, error) {
		// make sure this repo doesn't already exist
		_, err := tx.ReadRepo()
		if err == nil {
			// couldn't verify this bucket doesn't yet exist
			return nil, ErrRepoExists
		} else if !errors.Is(err, db.ErrNotFound) {
			index.log().WithError(err).Error("could not read repo")
			return nil, err // error reading the repo
		}

		err = tx.WriteRepo(repo)
		if err != nil {
			return nil, err
		}

		// write empty tree
		commit := &Commit{
			RepositoryId: repoId,
			Tree:         ident.Empty(),
			Committer:    "",
			Message:      "",
			CreationDate: creationDate,
			Parents:      []string{},
			Metadata:     make(map[string]string),
		}
		commitId := ident.Hash(commit)
		commit.Address = commitId
		err = tx.WriteCommit(commitId, commit)
		if err != nil {
			index.log().WithError(err).Error("could not write initial commit")
			return nil, err
		}
		err = tx.WriteBranch(repo.DefaultBranch, &Branch{
			Id:         repo.DefaultBranch,
			CommitId:   commitId,
			CommitRoot: commit.Tree,
		})
		if err != nil {
			index.log().WithError(err).Error("could not write branch")
		}
		return nil, err
	})
	return err
}

func (index *DBIndexer) ListRepos(amount int, after string) ([]*Repo, bool, error) {
	type result struct {
		repos   []*Repo
		hasMore bool
	}
	res, err := index.store.Transact(func(tx ClientOperations) (interface{}, error) {
		repos, hasMore, err := tx.ListRepos(amount, after)
		return &result{
			repos:   repos,
			hasMore: hasMore,
		}, err
	}, db.ReadOnly())
	if err != nil {
		index.log().WithError(err).Error("could not list repos")
		return nil, false, err
	}
	return res.(*result).repos, res.(*result).hasMore, nil
}

func (index *DBIndexer) GetRepo(repoId string) (*Repo, error) {
	err := ValidateAll(
		ValidateRepoId(repoId))
	if err != nil {
		return nil, err
	}
	repo, err := index.store.Transact(func(tx ClientOperations) (interface{}, error) {
		return tx.ReadRepo(repoId)
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	return repo.(*Repo), nil
}

func (index *DBIndexer) DeleteRepo(repoId string) error {
	err := ValidateAll(
		ValidateRepoId(repoId))
	if err != nil {
		return err
	}
	_, err = index.store.Transact(func(tx ClientOperations) (interface{}, error) {
		_, err := tx.ReadRepo(repoId)
		if err != nil {
			return nil, err
		}
		err = tx.DeleteRepo(repoId)
		if err != nil {
			index.log().WithError(err).Error("could not delete repo")
			return nil, err
		}
		return nil, nil
	})
	return err
}

func (index *DBIndexer) CreateDedupEntryIfNone(repoId string, dedupId string, objName string) (string, error) {
	objectId, err := index.store.RepoTransact(repoId, func(tx RepoOperations) (interface{}, error) {
		dedupObj, err := tx.GetObjectDedup(dedupId)
		if err == nil {
			return dedupObj.PhysicalAddress, nil
		} else if errors.Is(err, db.ErrNotFound) {
			d := &ObjectDedup{RepositoryId: repoId, PhysicalAddress: objName, DedupId: dedupId}
			err = tx.WriteObjectDedup(d)
			if err != nil {
				index.log().WithError(err).Error("failed writing dedup record")
			}
			return objName, err
		} else {
			index.log().WithError(err).Error("Error reading  dedup record")
			return objName, err
		}
	})
	val, ok := objectId.(string)
	if ok {
		return val, err
	} else {
		return objName, err
	}
}

func (index *DBIndexer) CreateMultiPartUpload(repoId, uploadId, path, objectName string, creationDate time.Time) error {
	_, err := index.store.RepoTransact(repoId, func(tx RepoOperations) (interface{}, error) {
		m := &MultipartUpload{RepositoryId: repoId, UploadId: uploadId, Path: path, CreationDate: creationDate, PhysicalAddress: objectName}
		err := tx.WriteMultipartUpload(m)
		return nil, err
	})
	return err
}

func (index *DBIndexer) ReadMultiPartUpload(repoId, uploadId string) (*MultipartUpload, error) {
	multi, err := index.store.RepoTransact(repoId, func(tx RepoOperations) (interface{}, error) {
		m, err := tx.ReadMultipartUpload(uploadId)
		if err != nil {
			index.log().WithError(err).Error("failed reading MultiPart record")
		}
		return m, err
	})

	if err == nil {
		return multi.(*MultipartUpload), err
	} else {
		return nil, err
	}
}

func (index *DBIndexer) DeleteMultiPartUpload(repoId, uploadId string) error {
	_, err := index.store.RepoTransact(repoId, func(tx RepoOperations) (interface{}, error) {
		err := tx.DeleteMultipartUpload(uploadId)
		return nil, err
	})
	return err
}

func (index *DBIndexer) DiffWorkspace(repoId, branch string) (Differences, error) {
	res, err := index.store.RepoTransact(repoId, func(tx RepoOperations) (i interface{}, err error) {
		var result Differences
		branchData, err := tx.ReadBranch(branch)
		if err != nil {
			return nil, err
		}
		tree := NewMerkle(branchData.CommitRoot)
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
	return res.(Differences), nil
}

// diffRecursive scans the workspace recursively and compares it to entries in the tree.
// It starts with the given WorkspaceEntry, and accumulates the diff in the result array.
func diffRecursive(tx RepoOperations, branch, parentPath, parentAddress string, tree Merkle, result *Differences) error {
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
			*result = append(*result, Difference{Type: DifferenceTypeAdded, Direction: DifferenceDirectionLeft, Path: currentWsEntry.Path, PathType: *currentWsEntry.EntryType})
		} else if currentWsEntry.TombstoneCount == currentEntry.ObjectCount {
			// deleted
			*result = append(*result, Difference{Type: DifferenceTypeRemoved, Direction: DifferenceDirectionLeft, Path: currentWsEntry.Path, PathType: *currentWsEntry.EntryType})
		} else if *currentWsEntry.EntryType == EntryTypeObject {
			// object: check if was changed
			if currentWsEntry.TombstoneCount == 0 && currentEntry.Checksum != *currentWsEntry.EntryChecksum {
				*result = append(*result, Difference{Type: DifferenceTypeChanged, Direction: DifferenceDirectionLeft, Path: currentWsEntry.Path, PathType: EntryTypeObject})
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
