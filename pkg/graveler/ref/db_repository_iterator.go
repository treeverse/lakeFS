package ref

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/graveler"
)

type DBRepositoryIterator struct {
	db        db.Database
	ctx       context.Context
	value     *graveler.RepositoryRecord
	buf       []*graveler.RepositoryRecord
	offset    string
	fetchSize int
	err       error
	state     iteratorState
}

func NewDBRepositoryIterator(ctx context.Context, db db.Database, fetchSize int) *DBRepositoryIterator {
	return &DBRepositoryIterator{
		db:        db,
		ctx:       ctx,
		fetchSize: fetchSize,
		buf:       make([]*graveler.RepositoryRecord, 0, fetchSize),
	}
}

func (ri *DBRepositoryIterator) Next() bool {
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

func (ri *DBRepositoryIterator) maybeFetch() {
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
	ri.err = ri.db.Select(ri.ctx, &ri.buf, `
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

func (ri *DBRepositoryIterator) SeekGE(id graveler.RepositoryID) {
	if errors.Is(ri.err, ErrIteratorClosed) {
		return
	}
	ri.offset = string(id)
	ri.buf = ri.buf[:0]
	ri.value = nil
	ri.err = nil
	ri.state = iteratorStateInit
}

func (ri *DBRepositoryIterator) Value() *graveler.RepositoryRecord {
	if ri.err != nil {
		return nil
	}
	return ri.value
}

func (ri *DBRepositoryIterator) Err() error {
	return ri.err
}

func (ri *DBRepositoryIterator) Close() {
	ri.err = ErrIteratorClosed
	ri.buf = nil
}
