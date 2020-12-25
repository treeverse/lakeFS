package ref

import (
	"context"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/graveler"
)

type RepositoryIterator struct {
	db  db.Database
	ctx context.Context

	value *graveler.RepositoryRecord
	buf   []*graveler.RepositoryRecord

	offset      string
	fetchSize   int
	shouldFetch bool

	// blockValue is true when the iterator is created, or SeekGE() is called.
	// A single Next() call turns it to false. When it's true, Value() returns nil.
	blockValue bool

	err error
}

func NewRepositoryIterator(ctx context.Context, db db.Database, fetchSize int) *RepositoryIterator {
	return &RepositoryIterator{
		db:          db,
		ctx:         ctx,
		fetchSize:   fetchSize,
		shouldFetch: true,
		blockValue:  true,
	}
}

func (ri *RepositoryIterator) Next() bool {
	ri.blockValue = false

	// no buffer is initialized
	if ri.buf == nil {
		ri.fetch(true) // initial fetch
	} else if len(ri.buf) == 0 {
		ri.fetch(false) // paginating since we're out of values
	}

	if len(ri.buf) == 0 {
		return false
	}

	// stage a value and increment offset
	ri.value = ri.buf[0]
	ri.offset = string(ri.value.RepositoryID)
	if len(ri.buf) > 1 {
		ri.buf = ri.buf[1:]
	} else {
		ri.buf = make([]*graveler.RepositoryRecord, 0)
	}

	return true
}

func (ri *RepositoryIterator) fetch(initial bool) {
	if !ri.shouldFetch {
		return
	}
	offsetCondition := iteratorOffsetCondition(initial)
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
		ri.shouldFetch = false
	}
}

func (ri *RepositoryIterator) SeekGE(id graveler.RepositoryID) {
	ri.offset = string(id)
	ri.shouldFetch = true
	ri.buf = nil
	ri.blockValue = true
}

func (ri *RepositoryIterator) Value() *graveler.RepositoryRecord {
	if ri.blockValue || ri.err != nil {
		return nil
	}
	return ri.value
}

func (ri *RepositoryIterator) Err() error {
	return ri.err
}

func (ri *RepositoryIterator) Close() {}
