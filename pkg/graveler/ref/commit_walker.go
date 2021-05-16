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
	// find the set of all ancestors of the left commit:
	leftAncestors := make(map[string]bool)
	for iterLeft.Next() {
		addr := addressProvider.ContentAddress(iterLeft.Value())
		leftAncestors[addr] = true
	}
	iterRight := NewCommitWalker(ctx, getter, repositoryID, right)
	// find the set of all common ancestors:
	commonAncestors := make(map[string]*graveler.Commit)
	for iterRight.Next() {
		commit := iterRight.Value()
		addr := addressProvider.ContentAddress(commit)
		if leftAncestors[addr] {
			commonAncestors[addr] = commit
		}
	}
	// on the subgraph containing only the common ancestors
	// count the number of children for each node
	children := make(map[string]int)
	for _, commit := range commonAncestors {
		for _, parent := range commit.Parents {
			children[parent.String()]++
		}
	}
	// the nodes with 0 children in the common graph qualify as lowest common ancestors
	for _, commit := range commonAncestors {
		addr := addressProvider.ContentAddress(commit)
		if children[addr] == 0 {
			return commonAncestors[addr]
		}
	}
	return nil
}
