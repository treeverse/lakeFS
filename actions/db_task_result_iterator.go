package actions

import (
	"context"
	"strings"

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

func NewDBTaskResultIterator(ctx context.Context, db db.Database, fetchSize int, repositoryID, runID, after string) *DBTaskResultIterator {
	it := &DBTaskResultIterator{
		db:           db,
		ctx:          ctx,
		repositoryID: repositoryID,
		runID:        runID,
		fetchSize:    fetchSize,
		buf:          make([]*TaskResult, 0, fetchSize),
	}
	it.actionNameOffset, it.hookIDOffset = parseTaskResultToken(after)
	return it
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
	it.actionNameOffset = it.value.ActionName
	it.hookIDOffset = it.value.HookID
	return true
}

func (it *DBTaskResultIterator) maybeFetch() {
	if it.state == iteratorStateDone {
		return
	}
	if len(it.buf) > 0 {
		return
	}

	if it.state == iteratorStateInit {
		it.state = iteratorStateQuery
	}

	q := psql.
		Select("run_id", "hook_id", "hook_type", "action_name", "start_time", "end_time", "passed").
		From("actions_run_hooks").
		Where(sq.Eq{"repository_id": it.repositoryID, "run_id": it.runID}).
		Where(sq.Gt{"action_name": it.actionNameOffset, "hook_id": it.hookIDOffset}).
		OrderBy("action_name", "hook_id").
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
		it.state = iteratorStateDone
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

func (it *DBTaskResultIterator) Token() string {
	if it.err != nil || it.value == nil {
		return ""
	}
	return it.value.ActionName + "/" + it.value.HookID
}

func parseTaskResultToken(token string) (actionName string, hookID string) {
	idx := strings.LastIndex(token, "/")
	if idx != -1 {
		actionName = token[:idx]
		hookID = token[idx+1:]
	}
	return
}
