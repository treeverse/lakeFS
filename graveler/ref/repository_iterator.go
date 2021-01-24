package ref

import (
	"context"
	"errors"

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
	if ri.err != nil {
		return false
	}

	ri.maybeFetch()

	// stage a value and increment offset
	if len(ri.buf) == 0 {
		return false
	}
	ri.value = ri.buf[0]
	ri.buf = ri.buf[1:]
	ri.offset = string(ri.value.RepositoryID)
	return true
}

func (ri *RepositoryIterator) maybeFetch() {
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
	if errors.Is(ri.err, ErrIteratorClosed) {
		return
	}
	ri.offset = string(id)
	ri.buf = ri.buf[:0]
	ri.value = nil
	ri.err = nil
	ri.state = iteratorStateInit
}

func (ri *RepositoryIterator) Value() *graveler.RepositoryRecord {
	if ri.err != nil {
		return nil
	}
	return ri.value
}

func (ri *RepositoryIterator) Err() error {
	return ri.err
}

func (ri *RepositoryIterator) Close() {
	ri.err = ErrIteratorClosed
	ri.buf = nil
}
