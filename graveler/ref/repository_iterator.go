package ref

import (
	"context"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/graveler"
)

type RepositoryIterator struct {
	db          db.Database
	ctx         context.Context
	value       *graveler.RepositoryRecord
	buf         []*graveler.RepositoryRecord
	offset      string
	fetchSize   int
	err         error
	fetchCalled bool
	fetchEnded  bool
	closed      bool
}

func NewRepositoryIterator(ctx context.Context, db db.Database, fetchSize int) *RepositoryIterator {
	return &RepositoryIterator{
		db:        db,
		ctx:       ctx,
		fetchSize: fetchSize,
	}
}

func (ri *RepositoryIterator) Next() bool {
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
	ri.offset = string(ri.value.RepositoryID)
	if len(ri.buf) > 1 {
		ri.buf = ri.buf[1:]
	} else {
		ri.buf = ri.buf[:0]
	}
	return true
}

func (ri *RepositoryIterator) fetch() {
	if ri.fetchEnded {
		return
	}
	offsetCondition := iteratorOffsetCondition(!ri.fetchCalled)
	ri.fetchCalled = true
	ri.err = ri.db.WithContext(ri.ctx).Select(&ri.buf, `
			SELECT id, storage_namespace, creation_date, default_branch
			FROM graveler_repositories
			WHERE id `+offsetCondition+` $1
			ORDER BY id ASC
			LIMIT $2`, ri.offset, ri.fetchSize)
	if ri.err != nil {
		return
	}
	if len(ri.buf) < ri.fetchSize {
		ri.fetchEnded = true
	}
}

func (ri *RepositoryIterator) SeekGE(id graveler.RepositoryID) {
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

func (ri *RepositoryIterator) Value() *graveler.RepositoryRecord {
	if ri.closed {
		panic(ErrIteratorClosed)
	}
	if ri.err != nil {
		return nil
	}
	return ri.value
}

func (ri *RepositoryIterator) Err() error {
	if ri.closed {
		panic(ErrIteratorClosed)
	}
	return ri.err
}

func (ri *RepositoryIterator) Close() {
	ri.closed = true
}
