package ref

import (
	"context"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/graveler"
)

type TagIterator struct {
	db           db.Database
	ctx          context.Context
	repositoryID graveler.RepositoryID
	value        *graveler.TagRecord
	buf          []*graveler.TagRecord
	offset       string
	fetchSize    int
	err          error
	state        iteratorState
}

type tagRecord struct {
	graveler.TagID    `db:"id"`
	graveler.CommitID `db:"commit_id"`
}

func NewTagIterator(ctx context.Context, db db.Database, repositoryID graveler.RepositoryID, fetchSize int) *TagIterator {
	return &TagIterator{
		db:           db,
		ctx:          ctx,
		repositoryID: repositoryID,
		fetchSize:    fetchSize,
		buf:          make([]*graveler.TagRecord, 0, fetchSize),
	}
}

func (ri *TagIterator) Next() bool {
	if ri.state == iteratorStateClosed {
		panic(ErrIteratorClosed)
	}
	if ri.err != nil {
		return false
	}

	ri.fetch()

	// stage a value and increment offset
	if len(ri.buf) == 0 {
		return false
	}
	ri.value = ri.buf[0]
	ri.offset = string(ri.value.TagID)
	if len(ri.buf) > 1 {
		ri.buf = ri.buf[1:]
	} else {
		ri.buf = ri.buf[:0]
	}
	return true
}

func (ri *TagIterator) fetch() {
	if ri.state == iteratorStateDone {
		return
	}
	if len(ri.buf) > 0 {
		return
	}

	var offsetCondition string
	if ri.state == iteratorStateInit {
		offsetCondition = iteratorOffsetCondition(true)
		ri.state = iteratorStateQuerying
	} else {
		offsetCondition = iteratorOffsetCondition(false)
	}

	var buf []*tagRecord
	err := ri.db.WithContext(ri.ctx).Select(&buf, `
			SELECT id, commit_id
			FROM graveler_tags
			WHERE repository_id = $1
			AND id `+offsetCondition+` $2
			ORDER BY id ASC
			LIMIT $3`, ri.repositoryID, ri.offset, ri.fetchSize)
	if err != nil {
		ri.err = err
		return
	}
	if len(buf) < ri.fetchSize {
		ri.state = iteratorStateDone
	}
	for _, b := range buf {
		ri.buf = append(ri.buf, &graveler.TagRecord{
			TagID:    b.TagID,
			CommitID: b.CommitID,
		})
	}
}

func (ri *TagIterator) SeekGE(id graveler.TagID) {
	if ri.state == iteratorStateClosed {
		panic(ErrIteratorClosed)
	}
	ri.offset = string(id)
	ri.buf = ri.buf[:0]
	ri.value = nil
	ri.err = nil
	ri.state = iteratorStateInit
}

func (ri *TagIterator) Value() *graveler.TagRecord {
	if ri.state == iteratorStateClosed {
		panic(ErrIteratorClosed)
	}
	if ri.err != nil {
		return nil
	}
	return ri.value
}

func (ri *TagIterator) Err() error {
	if ri.state == iteratorStateClosed {
		panic(ErrIteratorClosed)
	}
	return ri.err
}

func (ri *TagIterator) Close() {
	ri.state = iteratorStateClosed
}
