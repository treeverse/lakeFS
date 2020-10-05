package parade

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/treeverse/lakefs/logging"
)

var (
	ErrInvalidToken    = errors.New("performance token invalid (action may have exceeded deadline)")
	ErrBadStatus       = errors.New("bad status for task")
	ErrNoFinishChannel = errors.New("task has no Finishchannel")
)

type Parade interface {
	// InsertTasks adds tasks efficiently
	InsertTasks(ctx context.Context, tasks []TaskData) error

	// OwnTasks owns and returns up to maxTasks tasks for actor for performing any of
	// actions.  It will return tasks and for another OwnTasks call to acquire them after
	// maxDuration (if specified).
	OwnTasks(actor ActorID, maxTasks int, actions []string, maxDuration *time.Duration) ([]OwnedTaskData, error)

	// ExtendTaskDeadline extends the deadline for completing taskID which was acquired with
	// the specified token, for maxDuration longer.  It returns nil if the task is still
	// owned and its deadline was extended, or an SQL error, or ErrInvalidToken.  deadline
	// was extended.
	ExtendTaskDeadline(taskID TaskID, token PerformanceToken, maxDuration time.Duration) error

	// ReturnTask returns taskID which was acquired using the specified performanceToken,
	// giving it resultStatus and resultStatusCode.  It returns ErrInvalidToken if the
	// performanceToken is invalid; this happens when ReturnTask is called after its
	// deadline expires, or due to a logic error.
	ReturnTask(taskID TaskID, token PerformanceToken, resultStatus string, resultStatusCode TaskStatusCodeValue) error

	// NewWaiter returns TaskWaiter to wait for id on conn.  conn is owned by the returned
	// TaskWaiter until the waiter is done or cancelled.
	NewWaiter(ctx context.Context, taskID TaskID) (Waiter, error)

	// DeleteTasks deletes taskIDs, removing dependencies and deleting (effectively
	// recursively) any tasks that are left with no dependencies.  It creates a temporary
	// table on tx, so ideally close the transaction shortly after.  The effect is easiest
	// to analyze when all deleted tasks have been either completed or been aborted.
	DeleteTasks(ctx context.Context, taskIDs []TaskID) error
}

type Waiter interface {
	// Wait waits for the task to finish or the waiter to be cancelled and returns the task
	// status and status code.  It may safely be called from multiple goroutines.
	Wait() (string, TaskStatusCodeValue, error)

	// Cancel cancels waiting.
	Cancel()
}

type ParadeDB sqlx.DB

func NewParadeDB(db *sqlx.DB) Parade {
	return (*ParadeDB)(db)
}

func (p *ParadeDB) InsertTasks(ctx context.Context, tasks []TaskData) error {
	sqlConn, err := p.DB.Conn(ctx)
	if err != nil {
		return err
	}
	return sqlConn.Raw(func(driverConn interface{}) error {
		conn := driverConn.(*stdlib.Conn).Conn()
		return InsertTasks(ctx, conn, &TaskDataIterator{Data: tasks})
	})
}

func (p *ParadeDB) OwnTasks(actor ActorID, maxTasks int, actions []string, maxDuration *time.Duration) ([]OwnedTaskData, error) {
	return OwnTasks((*sqlx.DB)(p), actor, maxTasks, actions, maxDuration)
}

func (p *ParadeDB) ExtendTaskDeadline(taskID TaskID, token PerformanceToken, maxDuration time.Duration) error {
	return ExtendTaskDeadline((*sqlx.DB)(p), taskID, token, maxDuration)
}

func (p *ParadeDB) ReturnTask(taskID TaskID, token PerformanceToken, resultStatus string, resultStatusCode TaskStatusCodeValue) error {
	return ReturnTask((*sqlx.DB)(p), taskID, token, resultStatus, resultStatusCode)
}

func (p *ParadeDB) NewWaiter(ctx context.Context, taskID TaskID) (Waiter, error) {
	sqlConn, err := p.DB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("get conn to for waiter: %w", err)
	}
	defer sqlConn.Close()

	var ret *TaskDBWaiter
	// Ignore err, it is just returned along with ret

	// nolint:errcheck
	sqlConn.Raw(func(driverConn interface{}) error {
		conn := driverConn.(*stdlib.Conn).Conn()
		ret, err = NewWaiter(ctx, conn, taskID)
		return nil
	})
	return ret, err
}

func (p *ParadeDB) DeleteTasks(ctx context.Context, taskIDs []TaskID) error {
	sqlConn, err := p.DB.Conn(ctx)
	if err != nil {
		return err
	}
	return sqlConn.Raw(func(driverConn interface{}) error {
		conn := driverConn.(*stdlib.Conn).Conn()
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() {
			if tx != nil {
				// No useful error handling for an error here, return value is
				// already out there.  Just log.
				if err := tx.Rollback(ctx); err != nil {
					logging.FromContext(ctx).Errorf("rollback after error: %s", err)
				}
			}
		}()

		err = DeleteTasks(ctx, tx, taskIDs)
		if err != nil {
			return err
		}

		err = tx.Commit(ctx)
		if err != nil {
			// Try to rollback (it might not work or fail again, but at least we
			// tried...)
			return fmt.Errorf("COMMIT delete_tasks: %w", err)
		}
		tx = nil // Don't rollback
		return nil
	})
}
