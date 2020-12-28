package ref

import (
	"context"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/graveler"
)

type RepositoryIterator struct {
	db        db.Database
	ctx       context.Context
	value     *graveler.RepositoryRecord
	buf       []*graveler.RepositoryRecord
	offset    string
	fetchSize int
	err       error
	state     iteratorState
}

func NewRepositoryIterator(ctx context.Context, db db.Database, fetchSize int) *RepositoryIterator {
	return &RepositoryIterator{
		db:        db,
		ctx:       ctx,
		fetchSize: fetchSize,
		buf:       make([]*graveler.RepositoryRecord, 0, fetchSize),
	}
}

func (ri *RepositoryIterator) Next() bool {
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
	ri.offset = string(ri.value.RepositoryID)
	if len(ri.buf) > 1 {
		ri.buf = ri.buf[1:]
	} else {
		ri.buf = ri.buf[:0]
	}
	return true
}

func (ri *RepositoryIterator) fetch() {
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
		ri.state = iteratorStateDone
	}
}

func (ri *RepositoryIterator) SeekGE(id graveler.RepositoryID) {
	if ri.state == iteratorStateClosed {
		panic(ErrIteratorClosed)
	}
	ri.offset = string(id)
	ri.buf = ri.buf[:0]
	ri.value = nil
	ri.err = nil
	ri.state = iteratorStateInit
}

func (ri *RepositoryIterator) Value() *graveler.RepositoryRecord {
	if ri.state == iteratorStateClosed {
		panic(ErrIteratorClosed)
	}
	if ri.err != nil {
		return nil
	}
	return ri.value
}

func (ri *RepositoryIterator) Err() error {
	if ri.state == iteratorStateClosed {
		panic(ErrIteratorClosed)
	}
	return ri.err
}

func (ri *RepositoryIterator) Close() {
	ri.state = iteratorStateClosed
}
