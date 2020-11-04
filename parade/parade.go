package parade

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/treeverse/lakefs/logging"
)

var (
	ErrInvalidToken    = errors.New("performance token invalid (action may have exceeded deadline)")
	ErrBadStatus       = errors.New("bad status for task")
	ErrNoNotifyChannel = errors.New("task has no notify_channel_after")
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

type ParadeDB pgxpool.Pool

// NewParadeDB returns a Parade that implements Parade on a database.  The DDL should already be
// installed on that database.
func NewParadeDB(pool *pgxpool.Pool) Parade {
	return (*ParadeDB)(pool)
}

func (p *ParadeDB) pgxPool() *pgxpool.Pool {
	return (*pgxpool.Pool)(p)
}

func (p *ParadeDB) InsertTasks(ctx context.Context, tasks []TaskData) error {
	conn, err := p.pgxPool().Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()
	return InsertTasks(ctx, conn, &TaskDataIterator{Data: tasks})
}

func (p *ParadeDB) OwnTasks(actor ActorID, maxTasks int, actions []string, maxDuration *time.Duration) ([]OwnedTaskData, error) {
	ctx := context.Background()
	conn, err := p.pgxPool().Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	return OwnTasks(conn, actor, maxTasks, actions, maxDuration)
}

func (p *ParadeDB) ExtendTaskDeadline(taskID TaskID, token PerformanceToken, maxDuration time.Duration) error {
	ctx := context.Background()
	conn, err := p.pgxPool().Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	return ExtendTaskDeadline(conn, taskID, token, maxDuration)
}

func (p *ParadeDB) ReturnTask(taskID TaskID, token PerformanceToken, resultStatus string, resultStatusCode TaskStatusCodeValue) error {
	ctx := context.Background()
	conn, err := p.pgxPool().Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	return ReturnTask(conn, taskID, token, resultStatus, resultStatusCode)
}

func (p *ParadeDB) NewWaiter(ctx context.Context, taskID TaskID) (Waiter, error) {
	conn, err := p.pgxPool().Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("get conn to for waiter: %w", err)
	}

	// Transfers ownership of conn
	return NewWaiter(ctx, conn, taskID)
}

func (p *ParadeDB) DeleteTasks(ctx context.Context, taskIDs []TaskID) error {
	conn, err := p.pgxPool().Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()
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
}

// ParadePrefix wraps a Parade and adds a prefix to all TaskIDs, action names, and ActorIDs.
type ParadePrefix struct {
	Base   Parade
	Prefix string
}

func (pp *ParadePrefix) AddPrefix(s string) string {
	return fmt.Sprintf("%s.%s", pp.Prefix, s)
}

func (pp *ParadePrefix) StripPrefix(s string) string {
	return strings.TrimPrefix(s, pp.Prefix+".")
}

func (pp *ParadePrefix) AddPrefixTask(id TaskID) TaskID {
	return TaskID(pp.AddPrefix(string(id)))
}

func (pp *ParadePrefix) StripPrefixTask(id TaskID) TaskID {
	return TaskID(pp.StripPrefix(string(id)))
}

func (pp *ParadePrefix) AddPrefixActor(actor ActorID) ActorID {
	return ActorID(pp.AddPrefix(string(actor)))
}

func (pp *ParadePrefix) StripPrefixActor(actor TaskID) ActorID {
	return ActorID(pp.StripPrefix(string(actor)))
}

func (pp *ParadePrefix) InsertTasks(ctx context.Context, tasks []TaskData) error {
	prefixedTasks := make([]TaskData, len(tasks))
	for i := 0; i < len(tasks); i++ {
		copy := tasks[i]
		copy.ID = pp.AddPrefixTask(copy.ID)
		copy.Action = pp.AddPrefix(copy.Action)
		copy.ActorID = pp.AddPrefixActor(copy.ActorID)
		if copy.StatusCode == "" {
			copy.StatusCode = "pending"
		}
		toSignalAfter := make([]TaskID, len(copy.ToSignalAfter))
		for j := 0; j < len(toSignalAfter); j++ {
			toSignalAfter[j] = pp.AddPrefixTask(copy.ToSignalAfter[j])
		}
		copy.ToSignalAfter = toSignalAfter
		prefixedTasks[i] = copy
	}
	return pp.Base.InsertTasks(ctx, prefixedTasks)
}

func (pp *ParadePrefix) DeleteTasks(ctx context.Context, ids []TaskID) error {
	prefixedIDs := make([]TaskID, len(ids))
	for i := 0; i < len(ids); i++ {
		prefixedIDs[i] = pp.AddPrefixTask(ids[i])
	}

	if err := pp.Base.DeleteTasks(ctx, prefixedIDs); err != nil {
		return err
	}

	return nil
}

func (pp *ParadePrefix) ReturnTask(taskID TaskID, token PerformanceToken, resultStatus string, resultStatusCode TaskStatusCodeValue) error {
	return pp.Base.ReturnTask(pp.AddPrefixTask(taskID), token, resultStatus, resultStatusCode)
}

func (pp *ParadePrefix) ExtendTaskDeadline(taskID TaskID, token PerformanceToken, maxDuration time.Duration) error {
	return pp.Base.ExtendTaskDeadline(pp.AddPrefixTask(taskID), token, maxDuration)
}

func (pp *ParadePrefix) OwnTasks(actorID ActorID, maxTasks int, actions []string, maxDuration *time.Duration) ([]OwnedTaskData, error) {
	prefixedActions := make([]string, len(actions))
	for i, action := range actions {
		prefixedActions[i] = pp.AddPrefix(action)
	}
	tasks, err := pp.Base.OwnTasks(actorID, maxTasks, prefixedActions, maxDuration)
	if tasks != nil {
		for i := 0; i < len(tasks); i++ {
			task := &tasks[i]
			task.ID = pp.StripPrefixTask(task.ID)
			// TODO(ariels): Strip prefix from Action (so far unused in these tests)
		}
	}
	return tasks, err
}

func (pp *ParadePrefix) NewWaiter(ctx context.Context, taskID TaskID) (Waiter, error) {
	return pp.Base.NewWaiter(ctx, pp.AddPrefixTask(taskID))
}

var _ Parade = &ParadePrefix{}
