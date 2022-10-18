package ref

import (
	"container/heap"
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type CommitIterator struct {
	manager    graveler.RefManager
	ctx        context.Context
	repository *graveler.RepositoryRecord
	start      graveler.CommitID
	value      *graveler.CommitRecord
	queue      commitsPriorityQueue
	visit      map[graveler.CommitID]struct{}
	state      commitIteratorState
	err        error
}

type commitIteratorState int

const (
	commitIteratorStateInit commitIteratorState = iota
	commitIteratorStateQuery
	commitIteratorStateDone
)

type commitsPriorityQueue []*graveler.CommitRecord

func (c *commitsPriorityQueue) Len() int {
	return len(*c)
}

func (c *commitsPriorityQueue) Less(i, j int) bool {
	pq := *c
	if pq[i].Commit.CreationDate.Equal(pq[j].Commit.CreationDate) {
		return pq[i].CommitID > pq[j].CommitID
	}
	return pq[i].Commit.CreationDate.After(pq[j].Commit.CreationDate)
}

func (c *commitsPriorityQueue) Swap(i, j int) {
	pq := *c
	pq[i], pq[j] = pq[j], pq[i]
}

func (c *commitsPriorityQueue) Push(x interface{}) {
	rec := x.(*graveler.CommitRecord)
	*c = append(*c, rec)
}

func (c *commitsPriorityQueue) Pop() interface{} {
	cc := *c
	n := len(cc) - 1
	item := cc[n]
	*c = cc[:n]
	return item
}

// NewCommitIterator returns an iterator over all commits in the given repository.
// Ordering is based on the Commit Creation Date.
func NewCommitIterator(ctx context.Context, repository *graveler.RepositoryRecord, start graveler.CommitID, manager graveler.RefManager) *CommitIterator {
	return &CommitIterator{
		ctx:        ctx,
		repository: repository,
		start:      start,
		queue:      make(commitsPriorityQueue, 0),
		visit:      make(map[graveler.CommitID]struct{}),
		manager:    manager,
	}
}

func (ci *CommitIterator) getCommitRecord(commitID graveler.CommitID) (*graveler.CommitRecord, error) {
	commit, err := ci.manager.GetCommit(ci.ctx, ci.repository, commitID)
	if err != nil {
		return nil, err
	}
	return &graveler.CommitRecord{
		CommitID: commitID,
		Commit:   commit,
	}, nil
}

func (ci *CommitIterator) Next() bool {
	if ci.err != nil || ci.state == commitIteratorStateDone {
		return false
	}

	if ci.state == commitIteratorStateInit {
		// first time we look up the 'start' commit and push it into the queue
		ci.state = commitIteratorStateQuery
		rec, err := ci.getCommitRecord(ci.start)
		if err != nil {
			ci.value = nil
			ci.err = err
			return false
		}
		ci.queue.Push(rec)
	}

	// nothing in our queue - work is done
	if ci.queue.Len() == 0 {
		ci.value = nil
		ci.state = commitIteratorStateDone
		return false
	}

	// as long as we have something in the queue we will
	// set it as the current value and push the current commits parents to the queue
	ci.value = heap.Pop(&ci.queue).(*graveler.CommitRecord)
	for _, p := range ci.value.Parents {
		rec, err := ci.getCommitRecord(p)
		if err != nil {
			ci.value = nil
			ci.err = err
			return false
		}
		// skip commits we already visited
		if _, visited := ci.visit[rec.CommitID]; visited {
			continue
		}
		ci.visit[rec.CommitID] = struct{}{}
		heap.Push(&ci.queue, rec)
	}
	return true
}

// SeekGE skip under the point of 'id' commit ID based on a new
//   The list of commit
func (ci *CommitIterator) SeekGE(id graveler.CommitID) {
	ci.err = nil
	ci.queue = make(commitsPriorityQueue, 0)
	ci.visit = make(map[graveler.CommitID]struct{})
	ci.state = commitIteratorStateInit

	// skip until we get into our commit
	for ci.Next() {
		if ci.Value().CommitID == id {
			break
		}
	}
	if ci.Err() != nil {
		return
	}

	// step back - in order to have Next to read the value we just got,
	// we push back the current value to our queue and set the current value to nil.
	heap.Push(&ci.queue, ci.value)
	ci.value = nil
}

func (ci *CommitIterator) Value() *graveler.CommitRecord {
	return ci.value
}

func (ci *CommitIterator) Err() error {
	return ci.err
}

func (ci *CommitIterator) Close() {}
