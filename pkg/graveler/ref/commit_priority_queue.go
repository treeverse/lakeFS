package ref

import "github.com/treeverse/lakefs/pkg/graveler"

// CommitsPriorityQueue implements heap.Interface such that the commit with the oldest creation date is at the root
// of the heap.
type CommitsPriorityQueue struct {
	queue []*graveler.CommitRecord
}

func NewCommitsPriorityQueue() *CommitsPriorityQueue {
	return &CommitsPriorityQueue{queue: make([]*graveler.CommitRecord, 0)}
}

func (c CommitsPriorityQueue) Len() int {
	return len(c.queue)
}

func (c CommitsPriorityQueue) Less(i, j int) bool {
	if c.queue[i].Commit.CreationDate.Equal(c.queue[j].Commit.CreationDate) {
		return c.queue[i].CommitID > c.queue[j].CommitID
	}
	return c.queue[i].Commit.CreationDate.After(c.queue[j].Commit.CreationDate)
}

func (c CommitsPriorityQueue) Swap(i, j int) {
	c.queue[i], c.queue[j] = c.queue[j], c.queue[i]
}

func (c *CommitsPriorityQueue) Push(x interface{}) {
	rec := x.(*graveler.CommitRecord)
	c.queue = append(c.queue, rec)
}

func (c *CommitsPriorityQueue) Pop() interface{} {
	cc := c.queue
	n := len(cc) - 1
	item := cc[n]
	c.queue = cc[:n]
	return item
}

// CommitsGenerationPriorityQueue implements heap.Interface such that the commit with the greatest Generation value is
// at the root of the heap.
type CommitsGenerationPriorityQueue struct {
	*CommitsPriorityQueue
}

func NewCommitsGenerationPriorityQueue() *CommitsGenerationPriorityQueue {
	return &CommitsGenerationPriorityQueue{CommitsPriorityQueue: NewCommitsPriorityQueue()}
}

func (c CommitsGenerationPriorityQueue) Less(i, j int) bool {
	if c.queue[i].Commit.Generation == c.queue[j].Commit.Generation {
		c.queue[i].Commit.CreationDate.After(c.queue[j].Commit.CreationDate)
	}
	return c.queue[i].Commit.Generation > c.queue[j].Commit.Generation
}
