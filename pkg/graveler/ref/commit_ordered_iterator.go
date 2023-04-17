package ref

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

type OrderedCommitIterator struct {
	ctx                context.Context
	it                 *kv.PrimaryIterator
	store              kv.Store
	err                error
	value              *graveler.CommitRecord
	repositoryPath     string
	onlyAncestryLeaves bool
	firstParents       map[string]bool
}

// NewOrderedCommitIterator returns an iterator over all commits in the given repository.
// Ordering is based on the Commit ID value.
// WithOnlyAncestryLeaves causes the iterator to return only commits which are not the first parent of any other commit.
// Consider a commit graph where all non-first-parent edges are removed. This graph is a tree, and ancestry leaves are its leaves.
func NewOrderedCommitIterator(ctx context.Context, store kv.Store, repo *graveler.RepositoryRecord, onlyAncestryLeaves bool) (*OrderedCommitIterator, error) {
	repoPath := graveler.RepoPartition(repo)
	it, err := kv.NewPrimaryIterator(ctx, store, (&graveler.CommitData{}).ProtoReflect().Type(), repoPath,
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
	return &OrderedCommitIterator{
		ctx:                ctx,
		it:                 it,
		store:              store,
		repositoryPath:     repoPath,
		onlyAncestryLeaves: onlyAncestryLeaves,
		firstParents:       parents,
	}, nil
}

func (i *OrderedCommitIterator) Next() bool {
	if i.Err() != nil || i.it == nil {
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

func (i *OrderedCommitIterator) SeekGE(id graveler.CommitID) {
	if i.err != nil {
		return
	}
	i.it.Close()
	i.value = nil
	i.it, i.err = kv.NewPrimaryIterator(i.ctx, i.store, (&graveler.CommitData{}).ProtoReflect().Type(), i.repositoryPath,
		[]byte(graveler.CommitPath("")), kv.IteratorOptionsFrom([]byte(graveler.CommitPath(id))))
}

func (i *OrderedCommitIterator) Value() *graveler.CommitRecord {
	if i.Err() != nil {
		return nil
	}
	return i.value
}

func (i *OrderedCommitIterator) Err() error {
	if i.err != nil {
		return i.err
	}
	if i.it != nil {
		return i.it.Err()
	}
	return nil
}

func (i *OrderedCommitIterator) Close() {
	if i.it != nil {
		i.it.Close()
		i.it = nil
	}
}

// getAllFirstParents returns a set of all commits that are the first parent of some other commit for a given repository.
func getAllFirstParents(ctx context.Context, store kv.Store, repo *graveler.RepositoryRecord) (map[string]bool, error) {
	it, err := kv.NewPrimaryIterator(ctx, store, (&graveler.CommitData{}).ProtoReflect().Type(),
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
			parentNo := 0
			if graveler.CommitVersion(commit.Version) < graveler.CommitVersionParentSwitch && len(commit.Parents) > 1 {
				parentNo = 1
			}
			parent := commit.Parents[parentNo]
			firstParents[parent] = true
		}
	}
	return firstParents, nil
}
