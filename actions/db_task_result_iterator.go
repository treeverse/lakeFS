package actions

import (
	"context"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

type DBTaskResultIterator struct {
	db               db.Database
	ctx              context.Context
	value            *TaskResult
	buf              []*TaskResult
	fetchSize        int
	err              error
	state            iteratorState
	repositoryID     string
	runID            string
	actionNameOffset string
	hookIDOffset     string
}

func NewDBTaskResultIterator(ctx context.Context, db db.Database, fetchSize int, repositoryID, runID string) *DBTaskResultIterator {
	return &DBTaskResultIterator{
		db:           db,
		ctx:          ctx,
		repositoryID: repositoryID,
		runID:        runID,
		fetchSize:    fetchSize,
		buf:          make([]*TaskResult, 0, fetchSize),
	}
}

func (ri *DBTaskResultIterator) Next() bool {
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
	ri.actionNameOffset = ri.value.ActionName
	ri.hookIDOffset = ri.value.HookID
	return true
}

func (ri *DBTaskResultIterator) maybeFetch() {
	if ri.state == iteratorStateDone {
		return
	}
	if len(ri.buf) > 0 {
		return
	}

	if ri.state == iteratorStateInit {
		ri.state = iteratorStateQuery
	}

	q := psql.
		Select("run_id", "hook_id", "hook_type", "action_name", "start_time", "end_time", "passed").
		From("actions_run_hooks").
		Where(sq.Eq{"repository_id": ri.repositoryID, "run_id": ri.runID}).
		Where(sq.Gt{"action_name": ri.actionNameOffset, "hook_id": ri.hookIDOffset}).
		OrderBy("action_name", "hook_id").
		Limit(uint64(ri.fetchSize))

	var sql string
	var args []interface{}
	sql, args, ri.err = q.ToSql()
	if ri.err != nil {
		return
	}
	ri.err = ri.db.WithContext(ri.ctx).Select(&ri.buf, sql, args...)
	if ri.err != nil {
		return
	}
	if len(ri.buf) < ri.fetchSize {
		ri.state = iteratorStateDone
	}
}

func (ri *DBTaskResultIterator) Value() *TaskResult {
	if ri.err != nil {
		return nil
	}
	return ri.value
}

func (ri *DBTaskResultIterator) Err() error {
	return ri.err
}

func (ri *DBTaskResultIterator) Close() {
	ri.err = ErrIteratorClosed
	ri.buf = nil
}
