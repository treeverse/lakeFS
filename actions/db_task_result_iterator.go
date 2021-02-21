package actions

import (
	"context"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

type DBTaskResultIterator struct {
	db           db.Database
	ctx          context.Context
	value        *TaskResult
	buf          []*TaskResult
	done         bool
	fetchSize    int
	err          error
	repositoryID string
	runID        string
	offset       string
}

func NewDBTaskResultIterator(ctx context.Context, db db.Database, fetchSize int, repositoryID, runID, after string) *DBTaskResultIterator {
	return &DBTaskResultIterator{
		db:           db,
		ctx:          ctx,
		repositoryID: repositoryID,
		runID:        runID,
		fetchSize:    fetchSize,
		offset:       after,
		buf:          make([]*TaskResult, 0, fetchSize),
	}
}

func (it *DBTaskResultIterator) Next() bool {
	if it.err != nil {
		return false
	}

	it.maybeFetch()

	// stage a value and increment offset
	if len(it.buf) == 0 {
		return false
	}
	it.value = it.buf[0]
	it.buf = it.buf[1:]
	it.offset = it.value.HookRunID
	return true
}

func (it *DBTaskResultIterator) maybeFetch() {
	if it.done {
		return
	}
	if len(it.buf) > 0 {
		return
	}

	q := psql.
		Select("run_id", "hook_run_id", "hook_id", "action_name", "start_time", "end_time", "passed").
		From("actions_run_hooks").
		Where(sq.Eq{"repository_id": it.repositoryID, "run_id": it.runID}).
		Where(sq.Gt{"action_name": it.offset}).
		OrderBy("hook_run_id").
		Limit(uint64(it.fetchSize))

	var sql string
	var args []interface{}
	sql, args, it.err = q.ToSql()
	if it.err != nil {
		return
	}
	it.err = it.db.WithContext(it.ctx).Select(&it.buf, sql, args...)
	if it.err != nil {
		return
	}
	if len(it.buf) < it.fetchSize {
		it.done = true
	}
}

func (it *DBTaskResultIterator) Value() *TaskResult {
	if it.err != nil {
		return nil
	}
	return it.value
}

func (it *DBTaskResultIterator) Err() error {
	return it.err
}

func (it *DBTaskResultIterator) Close() {
	it.err = ErrIteratorClosed
	it.buf = nil
}
