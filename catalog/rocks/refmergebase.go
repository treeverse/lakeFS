package rocks

import (
	"context"

	"github.com/treeverse/lakefs/ident"
)

// taken from history:
// https://github.com/treeverse/lakeFS/blob/606bf07969c14a569a60efe9c92831f424fa7f36/index/dag/commit_iterator.go
type CommitGetter interface {
	GetCommit(ctx context.Context, repositoryID RepositoryID, commitID CommitID) (*Commit, error)
}

type CommitWalker struct {
	getter CommitGetter
	ctx    context.Context

	repositoryID RepositoryID

	queue         []CommitID
	discoveredSet map[CommitID]struct{}
	value         *Commit
	err           error
}

func NewCommitWalker(ctx context.Context, getter CommitGetter, repositoryID RepositoryID, startID CommitID) *CommitWalker {
	return &CommitWalker{
		getter:        getter,
		ctx:           ctx,
		repositoryID:  repositoryID,
		queue:         []CommitID{startID},
		discoveredSet: make(map[CommitID]struct{}),
	}
}

func (w *CommitWalker) Next() bool {
	if w.err != nil || len(w.queue) == 0 {
		w.value = nil
		return false // no more values to walk!
	}

	// pop
	addr := w.queue[0]
	w.queue = w.queue[1:]
	commit, err := w.getter.GetCommit(w.ctx, w.repositoryID, addr)
	if err != nil {
		w.err = err
		w.value = nil
		return false
	}

	// fill queue
	for _, parent := range commit.Parents {
		if _, wasDiscovered := w.discoveredSet[parent]; !wasDiscovered {
			w.queue = append(w.queue, parent)
			w.discoveredSet[parent] = struct{}{}
		}
	}
	w.value = commit
	return true
}

func (w *CommitWalker) Value() *Commit {
	return w.value
}

func (w *CommitWalker) Err() error {
	return w.err
}

func FindLowestCommonAncestor(ctx context.Context, getter CommitGetter, repositoryID RepositoryID, left, right CommitID) (*Commit, error) {
	discoveredSet := make(map[string]struct{})
	iterLeft := NewCommitWalker(ctx, getter, repositoryID, left)
	iterRight := NewCommitWalker(ctx, getter, repositoryID, right)
	for {
		commit, err := findLowerCommonAncestorNextIter(discoveredSet, iterLeft)
		if commit != nil || err != nil {
			return commit, err
		}
		commit, err = findLowerCommonAncestorNextIter(discoveredSet, iterRight)
		if commit != nil || err != nil {
			return commit, err
		}
		if iterLeft.Value() == nil && iterRight.Value() == nil {
			break
		}
	}
	return nil, nil
}

func findLowerCommonAncestorNextIter(discoveredSet map[string]struct{}, iter *CommitWalker) (*Commit, error) {
	if iter.Next() {
		commit := iter.Value()
		if _, wasDiscovered := discoveredSet[ident.ContentAddress(commit)]; wasDiscovered {
			return commit, nil
		}
		discoveredSet[ident.ContentAddress(commit)] = struct{}{}
	}
	return nil, iter.Err()
}
