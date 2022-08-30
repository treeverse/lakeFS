package ref

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

type KVOrderedCommitIterator struct {
	ctx                context.Context
	it                 *kv.PrimaryIterator
	store              kv.Store
	err                error
	value              *graveler.CommitRecord
	repositoryPath     string
	onlyAncestryLeaves bool
	firstParents       map[string]bool
}

// getAllFirstParents returns a set of all commits that are not a first parent of other commit in the given repository.
func getAllFirstParents(ctx context.Context, store *kv.StoreMessage, repo *graveler.RepositoryRecord) (map[string]bool, error) {
	it, err := kv.NewPrimaryIterator(ctx, store.Store, (&graveler.CommitData{}).ProtoReflect().Type(),
		graveler.RepoPartition(repo),
		[]byte(graveler.CommitPath("")), kv.IteratorOptionsFrom([]byte("")))
	if err != nil {
		return nil, err
	}
	defer it.Close()
	firstParents := make(map[string]bool)
	for it.Next() {
		entry := it.Entry()
		commit := entry.Value.(*graveler.CommitData)
		if len(commit.Parents) > 0 {
			if graveler.CommitVersion(commit.Version) < graveler.CommitVersionParentSwitch && len(commit.Parents) > 1 {
				firstParents[commit.Parents[1]] = true
			} else {
				firstParents[commit.Parents[0]] = true
			}
		}
	}
	return firstParents, nil
}

// NewKVOrderedCommitIterator returns an iterator over all commits in the given repository.
// Ordering is based on the Commit ID value.
// WithOnlyAncestryLeaves causes the iterator to return only commits which are not the first parent of any other commit.
// Consider a commit graph where all non-first-parent edges are removed. This graph is a tree, and ancestry leaves are its leaves.
func NewKVOrderedCommitIterator(ctx context.Context, store *kv.StoreMessage, repo *graveler.RepositoryRecord, onlyAncestryLeaves bool) (*KVOrderedCommitIterator, error) {
	repoPath := graveler.RepoPartition(repo)
	it, err := kv.NewPrimaryIterator(ctx, store.Store, (&graveler.CommitData{}).ProtoReflect().Type(), repoPath,
		[]byte(graveler.CommitPath("")), kv.IteratorOptionsFrom([]byte("")))
	if err != nil {
		return nil, err
	}
	var parents map[string]bool
	if onlyAncestryLeaves {
		parents, err = getAllFirstParents(ctx, store, repo)
		if err != nil {
			it.Close()
			return nil, err
		}
	}
	return &KVOrderedCommitIterator{
		ctx:                ctx,
		it:                 it,
		store:              store.Store,
		repositoryPath:     repoPath,
		onlyAncestryLeaves: onlyAncestryLeaves,
		firstParents:       parents,
	}, nil
}

func (i *KVOrderedCommitIterator) Next() bool {
	if i.Err() != nil {
		return false
	}
	for i.it.Next() {
		e := i.it.Entry()
		if e == nil {
			i.err = graveler.ErrInvalid
			return false
		}
		commit, ok := e.Value.(*graveler.CommitData)
		if commit == nil || !ok {
			i.err = graveler.ErrReadingFromStore
			return false
		}
		if !i.onlyAncestryLeaves || !i.firstParents[commit.Id] {
			i.value = CommitDataToCommitRecord(commit)
			return true
		}
	}
	i.value = nil
	return false
}

func (i *KVOrderedCommitIterator) SeekGE(id graveler.CommitID) {
	if i.err != nil {
		return
	}
	i.it.Close()
	i.value = nil
	i.it, i.err = kv.NewPrimaryIterator(i.ctx, i.store, (&graveler.CommitData{}).ProtoReflect().Type(), i.repositoryPath,
		[]byte(graveler.CommitPath("")), kv.IteratorOptionsFrom([]byte(graveler.CommitPath(id))))
}

func (i *KVOrderedCommitIterator) Value() *graveler.CommitRecord {
	if i.Err() != nil {
		return nil
	}
	return i.value
}

func (i *KVOrderedCommitIterator) Err() error {
	if i.err == nil {
		return i.it.Err()
	}
	return i.err
}

func (i *KVOrderedCommitIterator) Close() {
	if i.it != nil {
		i.it.Close()
	}
}
