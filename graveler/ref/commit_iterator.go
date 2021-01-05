package ref

import (
	"container/heap"
	"context"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/graveler"
)

type CommitIterator struct {
	db           db.Database
	ctx          context.Context
	repositoryID graveler.RepositoryID
	start        graveler.CommitID
	value        *graveler.CommitRecord
	queue        commitsPriorityQueue
	visit        map[graveler.CommitID]struct{}
	state        commitIteratorState
	err          error
}

type commitIteratorState int

const (
	commitIteratorStateInit commitIteratorState = iota
	commitIteratorStateQuery
	commitIteratorStateDone
)

type commitsPriorityQueue []*graveler.CommitRecord

func (c commitsPriorityQueue) Len() int {
	return len(c)
}

func (c commitsPriorityQueue) Less(i, j int) bool {
	if c[i].Commit.CreationDate.Equal(c[j].Commit.CreationDate) {
		return c[i].CommitID < c[j].CommitID
	}
	return c[i].Commit.CreationDate.After(c[j].Commit.CreationDate)
}

func (c commitsPriorityQueue) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
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

func NewCommitIterator(ctx context.Context, db db.Database, repositoryID graveler.RepositoryID, start graveler.CommitID) *CommitIterator {
	return &CommitIterator{
		db:           db,
		ctx:          ctx,
		repositoryID: repositoryID,
		start:        start,
		queue:        make(commitsPriorityQueue, 0),
		visit:        make(map[graveler.CommitID]struct{}),
	}
}

func (ci *CommitIterator) getCommitRecord(commitID graveler.CommitID) (*graveler.CommitRecord, error) {
	var rec commitRecord
	err := ci.db.WithContext(ci.ctx).
		Get(&rec, `SELECT id, committer, message, creation_date, parents, meta_range_id, metadata
			FROM graveler_commits
			WHERE repository_id = $1 AND id = $2`,
			ci.repositoryID, commitID)
	if err != nil {
		return nil, err
	}
	return rec.toGravelerCommitRecord(), nil
}

func (ci *CommitIterator) Next() bool {
	if ci.err != nil || ci.state == commitIteratorStateDone {
		return false
	}

	if ci.state == commitIteratorStateInit {
		ci.state = commitIteratorStateQuery
		rec, err := ci.getCommitRecord(ci.start)
		if err != nil {
			ci.err = err
			return false
		}
		ci.queue.Push(rec)
	}

	if ci.queue.Len() == 0 {
		ci.value = nil
		ci.state = commitIteratorStateDone
		return false
	}

	ci.value = heap.Pop(&ci.queue).(*graveler.CommitRecord)
	for _, p := range ci.value.Parents {
		rec, err := ci.getCommitRecord(p)
		if err != nil {
			ci.err = err
			ci.value = nil
			return false
		}
		if _, visited := ci.visit[rec.CommitID]; visited {
			continue
		}
		ci.visit[rec.CommitID] = struct{}{}
		heap.Push(&ci.queue, rec)
	}
	return true
}

func (ci *CommitIterator) SeekGE(id graveler.CommitID) {
	// Setting value to nil so that next Value() call
	// returns nil as the interface commands
	ci.value = nil
	ci.err = nil
	ci.start = id
	ci.queue = make(commitsPriorityQueue, 0)
	ci.visit = make(map[graveler.CommitID]struct{})
	ci.state = commitIteratorStateInit
}

func (ci *CommitIterator) Value() *graveler.CommitRecord {
	if ci.err != nil {
		return nil
	}
	return ci.value
}

func (ci *CommitIterator) Err() error {
	return ci.err
}

func (ci *CommitIterator) Close() {}
