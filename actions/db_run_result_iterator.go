package actions

import (
	"context"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

type DBRunResultIterator struct {
	db           db.Database
	ctx          context.Context
	value        *RunResult
	buf          []*RunResult
	offset       string
	fetchSize    int
	err          error
	done         bool
	repositoryID string
	branchID     *string
}

func NewDBRunResultIterator(ctx context.Context, db db.Database, fetchSize int, repositoryID string, branchID *string, after string) *DBRunResultIterator {
	return &DBRunResultIterator{
		db:           db,
		ctx:          ctx,
		repositoryID: repositoryID,
		offset:       after,
		branchID:     branchID,
		fetchSize:    fetchSize,
		buf:          make([]*RunResult, 0, fetchSize),
	}
}

func (it *DBRunResultIterator) Next() bool {
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
	it.offset = it.value.RunID
	return true
}

func (it *DBRunResultIterator) maybeFetch() {
	if it.done {
		return
	}
	if len(it.buf) > 0 {
		return
	}

	q := psql.
		Select("run_id", "event_type", "start_time", "end_time", "branch_id", "source_ref", "commit_id", "passed").
		From("actions_runs").
		Where(sq.Eq{"repository_id": it.repositoryID}).
		OrderBy("run_id DESC").
		Limit(uint64(it.fetchSize))
	if it.branchID != nil {
		q = q.Where(sq.Eq{"branch_id": *it.branchID})
	}
	if it.offset != "" {
		q = q.Where(sq.Lt{"run_id": it.offset})
	}

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

func (it *DBRunResultIterator) Value() *RunResult {
	if it.err != nil {
		return nil
	}
	return it.value
}

func (it *DBRunResultIterator) Err() error {
	return it.err
}

func (it *DBRunResultIterator) Close() {
	it.err = ErrIteratorClosed
	it.buf = nil
}
