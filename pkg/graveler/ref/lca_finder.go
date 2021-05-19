package ref

import (
	"container/heap"
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
)

// taken from history:
// https://github.com/treeverse/lakeFS/blob/606bf07969c14a569a60efe9c92831f424fa7f36/index/dag/commit_iterator.go
type CommitGetter interface {
	GetCommit(ctx context.Context, repositoryID graveler.RepositoryID, commitID graveler.CommitID) (*graveler.Commit, error)
}

type discoveredFlags struct {
	fromLeft  bool
	fromRight bool
}

func (d discoveredFlags) or(fromLeft bool, fromRight bool) discoveredFlags {
	d.fromLeft = d.fromLeft || fromLeft
	d.fromRight = d.fromRight || fromRight
	return d
}

type LCAFinder struct {
	getter       CommitGetter
	ctx          context.Context
	repositoryID graveler.RepositoryID
	queue        *CommitsGenerationPriorityQueue
	discovered   map[graveler.CommitID]discoveredFlags
}

func NewLCAFinder(ctx context.Context, getter CommitGetter, repositoryID graveler.RepositoryID, leftID, rightID graveler.CommitID) (*LCAFinder, error) {
	queue := NewCommitsGenerationPriorityQueue()
	for _, commitID := range []graveler.CommitID{leftID, rightID} {
		commit, err := getter.GetCommit(ctx, repositoryID, commitID)
		if err != nil {
			return nil, err
		}
		heap.Push(queue, &graveler.CommitRecord{
			CommitID: commitID,
			Commit:   commit,
		})
	}
	discovered := make(map[graveler.CommitID]discoveredFlags)
	discovered[leftID] = discoveredFlags{fromLeft: true}
	discovered[rightID] = discoveredFlags{fromRight: true}
	return &LCAFinder{
		getter:       getter,
		ctx:          ctx,
		repositoryID: repositoryID,
		queue:        queue,
		discovered:   discovered,
	}, nil
}

func (w *LCAFinder) Find() (*graveler.Commit, error) {
	var commitRecord *graveler.CommitRecord
	var commitFlags discoveredFlags
	for {
		commitRecord = heap.Pop(w.queue).(*graveler.CommitRecord)
		if w.queue.Len() == 0 {
			return nil, nil
		}
		commitFlags = w.discovered[commitRecord.CommitID]
		if commitFlags.fromLeft && commitFlags.fromRight {
			return commitRecord.Commit, nil
		}
		// fill queue
		for _, parent := range commitRecord.Parents {
			parentCommit, err := w.getter.GetCommit(w.ctx, w.repositoryID, parent)
			if err != nil {
				return nil, err
			}
			heap.Push(w.queue, &graveler.CommitRecord{
				CommitID: parent,
				Commit:   parentCommit,
			})
			if _, ok := w.discovered[parent]; !ok {
				w.discovered[parent] = discoveredFlags{}
			}
			w.discovered[parent] = w.discovered[parent].or(commitFlags.fromLeft, commitFlags.fromRight)
		}
	}
}

func FindLowestCommonAncestor(ctx context.Context, getter CommitGetter, repositoryID graveler.RepositoryID, left, right graveler.CommitID) (*graveler.Commit, error) {
	lcaFinder, err := NewLCAFinder(ctx, getter, repositoryID, left, right)
	if err != nil {
		return nil, err
	}
	return lcaFinder.Find()
}
