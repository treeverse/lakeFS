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
	// blockValue is true when the iterator is created, or SeekGE() is called.
	// A single Next() call turns it to false. When it's true, Value() returns nil.
	blockValue  bool
	offset      string
	fetchSize   int
	shouldFetch bool
	err         error
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
		shouldFetch:  true,
		blockValue:   true,
		offset:       offset,
	}
}

func (ri *TagIterator) Next() bool {
	ri.blockValue = false

	// no buffer is initialized
	if ri.buf == nil {
		ri.fetch(true) // initial fetch
	} else if len(ri.buf) == 0 {
		ri.fetch(false) // paging size we're out of values
	}

	if len(ri.buf) == 0 {
		return false
	}

	// stage a value and increment offset
	ri.value = ri.buf[0]
	ri.offset = string(ri.value.CommitID)
	if len(ri.buf) > 1 {
		ri.buf = ri.buf[1:]
	} else {
		ri.buf = ri.buf[:0]
	}
	return true
}

func (ri *TagIterator) fetch(initial bool) {
	if !ri.shouldFetch {
		return
	}
	offsetCondition := iteratorOffsetCondition(initial)
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
		ri.shouldFetch = false
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
	ri.offset = string(id)
	ri.shouldFetch = true
	ri.buf = nil
	ri.blockValue = true
}

func (ri *TagIterator) Value() *graveler.TagRecord {
	if ri.blockValue || ri.err != nil {
		return nil
	}
	return ri.value
}

func (ri *TagIterator) Err() error {
	return ri.err
}

func (ri *TagIterator) Close() {}
