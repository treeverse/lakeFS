package ref

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/graveler"
)

// NewOrderedCommitIterator returns an iterator over all commits in the given repository.
// Ordering is based on the Commit ID value.
func NewOrderedCommitIterator(ctx context.Context, database db.Database, repositoryID graveler.RepositoryID, prefetchSize int) (*OrderedCommitIterator, error) {
	return &OrderedCommitIterator{
		ctx:          ctx,
		db:           database,
		repositoryID: repositoryID,
		prefetchSize: prefetchSize,
		buf:          make([]*graveler.CommitRecord, 0, prefetchSize),
	}, nil
}

type OrderedCommitIterator struct {
	ctx          context.Context
	db           db.Database
	repositoryID graveler.RepositoryID
	prefetchSize int
	buf          []*graveler.CommitRecord
	err          error
	value        *graveler.CommitRecord
	offset       string
	state        iteratorState
}

func (iter *OrderedCommitIterator) Next() bool {
	if iter.err != nil {
		return false
	}
	iter.maybeFetch()

	// stage a value and increment offset
	if len(iter.buf) == 0 {
		return false
	}
	iter.value = iter.buf[0]
	iter.buf = iter.buf[1:]
	iter.offset = string(iter.value.CommitID)
	return true
}

func (iter *OrderedCommitIterator) maybeFetch() {
	if iter.state == iteratorStateDone {
		return
	}
	if len(iter.buf) > 0 {
		return
	}

	var offsetCondition string
	if iter.state == iteratorStateInit {
		offsetCondition = iteratorOffsetCondition(true)
		iter.state = iteratorStateQuerying
	} else {
		offsetCondition = iteratorOffsetCondition(false)
	}

	var buf []*commitRecord
	err := iter.db.Select(iter.ctx, &buf, `
			SELECT id, committer, message, creation_date, meta_range_id, parents, metadata
			FROM graveler_commits
			WHERE repository_id = $1
			AND id `+offsetCondition+` $2
			ORDER BY id ASC
			LIMIT $3`, iter.repositoryID, iter.offset, iter.prefetchSize)
	if err != nil {
		iter.err = err
		return
	}
	if len(buf) < iter.prefetchSize {
		iter.state = iteratorStateDone
	}
	for _, c := range buf {
		rec := c.toGravelerCommitRecord()
		iter.buf = append(iter.buf, rec)
	}
}

func (iter *OrderedCommitIterator) SeekGE(id graveler.CommitID) {
	if errors.Is(iter.err, ErrIteratorClosed) {
		return
	}
	iter.offset = string(id)
	iter.state = iteratorStateInit
	iter.buf = iter.buf[:0]
	iter.value = nil
	iter.err = nil
}

func (iter *OrderedCommitIterator) Value() *graveler.CommitRecord {
	if iter.err != nil {
		return nil
	}
	return iter.value
}

func (iter *OrderedCommitIterator) Err() error {
	return iter.err
}

func (iter *OrderedCommitIterator) Close() {
	iter.err = ErrIteratorClosed
	iter.buf = nil
}
