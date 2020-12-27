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
	fetchCalled  bool
	fetchEnded   bool
	closed       bool
}

type tagRecord struct {
	graveler.TagID    `db:"id"`
	graveler.CommitID `db:"commit_id"`
}

func NewTagIterator(ctx context.Context, db db.Database, repositoryID graveler.RepositoryID, prefetchSize int, offset string) *TagIterator {
	return &TagIterator{
		db:           db,
		ctx:          ctx,
		repositoryID: repositoryID,
		fetchSize:    prefetchSize,
		offset:       offset,
	}
}

func (ri *TagIterator) Next() bool {
	if ri.closed {
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
	ri.offset = string(ri.value.CommitID)
	if len(ri.buf) > 1 {
		ri.buf = ri.buf[1:]
	} else {
		ri.buf = ri.buf[:0]
	}
	return true
}

func (ri *TagIterator) fetch() {
	if ri.fetchEnded {
		return
	}
	offsetCondition := iteratorOffsetCondition(!ri.fetchCalled)
	ri.fetchCalled = true

	buf := make([]*tagRecord, 0)
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
		ri.fetchEnded = true
	}
	ri.buf = make([]*graveler.TagRecord, len(buf))
	for i, b := range buf {
		ri.buf[i] = &graveler.TagRecord{
			TagID:    b.TagID,
			CommitID: b.CommitID,
		}
	}
}

func (ri *TagIterator) SeekGE(id graveler.TagID) {
	if ri.closed {
		panic(ErrIteratorClosed)
	}
	ri.offset = string(id)
	ri.buf = nil
	ri.value = nil
	ri.err = nil
	ri.fetchCalled = false
	ri.fetchEnded = false
}

func (ri *TagIterator) Value() *graveler.TagRecord {
	if ri.closed {
		panic(ErrIteratorClosed)
	}
	if ri.err != nil {
		return nil
	}
	return ri.value
}

func (ri *TagIterator) Err() error {
	if ri.closed {
		panic(ErrIteratorClosed)
	}
	return ri.err
}

func (ri *TagIterator) Close() {
	ri.closed = true
}
