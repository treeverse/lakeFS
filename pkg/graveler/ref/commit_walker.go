package ref

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/ident"
)

// taken from history:
// https://github.com/treeverse/lakeFS/blob/606bf07969c14a569a60efe9c92831f424fa7f36/index/dag/commit_iterator.go
type CommitGetter interface {
	GetCommit(ctx context.Context, repositoryID graveler.RepositoryID, commitID graveler.CommitID) (*graveler.Commit, error)
}

type CommitWalker struct {
	getter        CommitGetter
	ctx           context.Context
	repositoryID  graveler.RepositoryID
	queue         []graveler.CommitID
	discoveredSet map[graveler.CommitID]struct{}
	value         *graveler.Commit
	err           error
}

func NewCommitWalker(ctx context.Context, getter CommitGetter, repositoryID graveler.RepositoryID, startID graveler.CommitID) *CommitWalker {
	return &CommitWalker{
		getter:        getter,
		ctx:           ctx,
		repositoryID:  repositoryID,
		queue:         []graveler.CommitID{startID},
		discoveredSet: make(map[graveler.CommitID]struct{}),
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

func (w *CommitWalker) Value() *graveler.Commit {
	return w.value
}

func (w *CommitWalker) Err() error {
	return w.err
}

func FindLowestCommonAncestor(ctx context.Context, getter CommitGetter, addressProvider ident.AddressProvider, repositoryID graveler.RepositoryID, left, right graveler.CommitID) *graveler.Commit {
	iterLeft := NewCommitWalker(ctx, getter, repositoryID, left)
	reds := make(map[string]bool)
	for iterLeft.Next() {
		addr := addressProvider.ContentAddress(iterLeft.Value())
		reds[addr] = true
	}
	iterRight := NewCommitWalker(ctx, getter, repositoryID, right)
	blacks := make(map[string]*graveler.Commit)
	for iterRight.Next() {
		commit := iterRight.Value()
		addr := addressProvider.ContentAddress(commit)
		if reds[addr] {
			blacks[addr] = commit
		}
	}
	degrees := make(map[string]int)
	for _, commit := range blacks {
		for _, parent := range commit.Parents {
			degrees[parent.String()]++
		}
	}
	for _, commit := range blacks {
		addr := addressProvider.ContentAddress(commit)
		if degrees[addr] == 0 {
			return blacks[addr]
		}
	}
	return nil
}
