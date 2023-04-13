package ref

import (
	"github.com/treeverse/lakefs/pkg/graveler"
)

// CommitsGenerationPriorityQueue implements heap.Interface such that the commit with the greatest Generation value is
// at the root of the heap.
type CommitsGenerationPriorityQueue []*graveler.CommitRecord

func NewCommitsGenerationPriorityQueue() CommitsGenerationPriorityQueue {
	return make(CommitsGenerationPriorityQueue, 0)
}

func (c CommitsGenerationPriorityQueue) Len() int {
	return len(c)
}

func (c CommitsGenerationPriorityQueue) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (c *CommitsGenerationPriorityQueue) Push(x interface{}) {
	rec := x.(*graveler.CommitRecord)
	*c = append(*c, rec)
}

func (c *CommitsGenerationPriorityQueue) Pop() interface{} {
	cc := *c
	n := len(cc) - 1
	item := cc[n]
	*c = cc[:n]
	return item
}

func (c CommitsGenerationPriorityQueue) Less(i, j int) bool {
	if c[i].Commit.Generation == c[j].Commit.Generation {
		return c[i].Commit.CreationDate.After(c[j].Commit.CreationDate)
	}
	return c[i].Commit.Generation > c[j].Commit.Generation
}
