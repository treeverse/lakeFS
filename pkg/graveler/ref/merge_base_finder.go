package ref

import (
	"container/heap"
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type CommitGetter interface {
	GetCommit(ctx context.Context, repositoryID graveler.RepositoryID, commitID graveler.CommitID) (*graveler.Commit, error)
}

type reachedFlags uint8

const (
	fromLeft reachedFlags = 1 << iota
	fromRight
)

// FindMergeBase finds the best common ancestor according to the definition in the git-merge-base documentation: https://git-scm.com/docs/git-merge-base
// One common ancestor is better than another common ancestor if the latter is an ancestor of the former.
func FindMergeBase(ctx context.Context, getter CommitGetter, repositoryID graveler.RepositoryID, leftID, rightID graveler.CommitID) (*graveler.Commit, error) {
	var commitRecord *graveler.CommitRecord
	queue := NewCommitsGenerationPriorityQueue()
	reached := make(map[graveler.CommitID]reachedFlags)
	reached[rightID] |= fromRight
	reached[leftID] |= fromLeft
	// create an hypothetical commit with given nodes as parents, and insert it to the queue
	heap.Push(&queue, &graveler.CommitRecord{
		Commit: &graveler.Commit{Parents: []graveler.CommitID{leftID, rightID}},
	})
	for {
		if queue.Len() == 0 {
			return nil, nil
		}
		commitRecord = heap.Pop(&queue).(*graveler.CommitRecord)
		commitFlags := reached[commitRecord.CommitID]
		if commitFlags&fromLeft != 0 && commitFlags&fromRight != 0 {
			// commit was reached from both left and right nodes
			return commitRecord.Commit, nil
		}
		for _, parent := range commitRecord.Parents {
			parentCommit, err := getter.GetCommit(ctx, repositoryID, parent)
			if err != nil {
				return nil, err
			}
			heap.Push(&queue, &graveler.CommitRecord{CommitID: parent, Commit: parentCommit})
			// mark the parent with the flag values from its descendents:
			reached[parent] |= commitFlags
		}
	}
}
