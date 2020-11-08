package parade

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	nanoid "github.com/matoous/go-nanoid"
	"github.com/treeverse/lakefs/logging"
)

type TaskID string

type ActorID string

type PerformanceToken pgtype.UUID

// nolint: stylecheck
func (dst *PerformanceToken) Scan(src interface{}) error {
	var scanned pgtype.UUID
	if err := scanned.Scan(src); err != nil {
		return err
	}
	*dst = PerformanceToken(scanned)
	return nil
}

func (src PerformanceToken) Value() (driver.Value, error) {
	return pgtype.UUID(src).Value()
}

func (src PerformanceToken) String() string {
	res := strings.Builder{}
	offset := 0
	addBytes := func(n int) {
		for i := 0; i < n; i++ {
			res.WriteString(fmt.Sprintf("%02x", src.Bytes[offset+i]))
		}
		offset += n
	}
	// nolint:gomnd
	addBytes(4)
	res.WriteString("-")
	// nolint:gomnd
	addBytes(2)
	res.WriteString("-")
	// nolint:gomnd
	addBytes(2)
	res.WriteString("-")
	// nolint:gomnd
	addBytes(2)
	res.WriteString("-")
	// nolint:gomnd
	addBytes(6)
	return res.String()
}

type TaskStatusCodeValue string

const (
	// TaskPending indicates a task is waiting for an actor to perform it (new or being
	// retried)
	TaskPending TaskStatusCodeValue = "pending"
	// TaskInProgress indicates a task is being performed by an actor.
	TaskInProgress TaskStatusCodeValue = "in-progress"
	// TaskAborted indicates an actor has aborted this task with message, will not be reissued
	TaskAborted TaskStatusCodeValue = "aborted"
	// TaskCompleted indicates an actor has completed this task with message, will not reissued
	TaskCompleted TaskStatusCodeValue = "completed"
	// TaskInvalid is used by the API to report errors
	TaskInvalid TaskStatusCodeValue = "[invalid]"
)

var ErrBadTypeConversion = errors.New("bad type")

// nolint: stylecheck
func (dst *TaskStatusCodeValue) Scan(src interface{}) error {
	var sc TaskStatusCodeValue
	switch s := src.(type) {
	case string:
		sc = TaskStatusCodeValue(strings.ToLower(s))
	case []byte:
		sc = TaskStatusCodeValue(strings.ToLower(string(s)))
	default:
		return fmt.Errorf("cannot convert %T to TaskStatusCodeValue: %w", src, ErrBadTypeConversion)
	}

	if !(sc == TaskPending || sc == TaskInProgress || sc == TaskAborted || sc == TaskCompleted) {
		// not a failure, "just" be a newer enum value than known
		*dst = TaskInvalid
		return nil
	}
	*dst = sc
	return nil
}

func (src TaskStatusCodeValue) Value() (driver.Value, error) {
	return string(src), nil
}

// TaskData is a row in table "tasks".  It describes a task to perform.
type TaskData struct {
	ID     TaskID `db:"task_id"`
	Action string `db:"action"`
	// Body is JSON-formatted
	Body               *string             `db:"body"`
	Status             *string             `db:"status"`
	StatusCode         TaskStatusCodeValue `db:"status_code"`
	NumTries           int                 `db:"num_tries"`
	NumFailures        int                 `db:"num_failures"`
	MaxTries           *int                `db:"max_tries"`
	NumSignals         int                 `db:"num_signals"` // Internal; set (only) in tests
	TotalDependencies  *int                `db:"total_dependencies"`
	ActorID            ActorID             `db:"actor_id"`
	ActionDeadline     *time.Time          `db:"action_deadline"`
	PerformanceToken   *PerformanceToken   `db:"performance_token"`
	ToSignalAfter      []TaskID            `db:"to_signal_after"`
	NotifyChannelAfter *string             `db:"notify_channel_after"`
}

// TaskDataIterator implements the pgx.CopyFromSource interface and allows using CopyFrom to insert
// multiple TaskData rapidly.
type TaskDataIterator struct {
	Data []TaskData
	next int
	err  error
}

func (td *TaskDataIterator) Next() bool {
	if td.next > len(td.Data) {
		td.err = ErrNoMoreData
		return false
	}
	ret := td.next < len(td.Data)
	td.next++
	return ret
}

var ErrNoMoreData = errors.New("no more data")

func (td *TaskDataIterator) Err() error {
	return td.err
}

func (td *TaskDataIterator) Values() ([]interface{}, error) {
	if td.next > len(td.Data) {
		td.err = ErrNoMoreData
		return nil, td.err
	}
	value := td.Data[td.next-1]
	// Convert ToSignalAfter to a text array so pgx can convert it to text.  Needed because
	// Go types.
	toSignalAfter := make([]string, len(value.ToSignalAfter))
	for i := 0; i < len(value.ToSignalAfter); i++ {
		toSignalAfter[i] = string(value.ToSignalAfter[i])
	}
	return []interface{}{
		value.ID,
		value.Action,
		value.Body,
		value.Status,
		value.StatusCode,
		value.NumTries,
		value.MaxTries,
		value.NumSignals,
		value.TotalDependencies,
		value.ActorID,
		value.ActionDeadline,
		value.PerformanceToken,
		toSignalAfter,
		value.NotifyChannelAfter,
	}, nil
}

// Names of columns in tasks as they appear in the database.
var TaskDataColumnNames = []string{
	"id", "action", "body", "status", "status_code", "num_tries", "max_tries",
	"num_signals", "total_dependencies",
	"actor_id", "action_deadline", "performance_token",
	"to_signal_after", "notify_channel_after",
}

var tasksTable = pgx.Identifier{"tasks"}

func InsertTasks(ctx context.Context, conn *pgxpool.Conn, source pgx.CopyFromSource) error {
	_, err := conn.CopyFrom(ctx, tasksTable, TaskDataColumnNames, source)
	return err
}

// OwnedTaskData is a row returned from "SELECT * FROM own_tasks(...)".
type OwnedTaskData struct {
	ID                   TaskID           `db:"task_id"`
	Token                PerformanceToken `db:"token"`
	NumSignalledFailures int              `db:"num_failures"`
	Action               string           `db:"action"`
	Body                 *string
}

// OwnTasks owns for actor and returns up to maxTasks tasks for performing any of actions.
func OwnTasks(conn pgxscan.Querier, actor ActorID, maxTasks int, actions []string, maxDuration *time.Duration) ([]OwnedTaskData, error) {
	ctx := context.Background()
	rows, err := conn.Query(
		ctx, `SELECT * FROM own_tasks($1, $2, $3, $4)`, maxTasks, actions, actor, maxDuration)
	if err != nil {
		return nil, fmt.Errorf("try to own tasks: %w", err)
	}
	tasks := make([]OwnedTaskData, 0, maxTasks)
	err = pgxscan.ScanAll(&tasks, rows)
	return tasks, err
}

// ExtendTaskDeadline extends the deadline for completing taskID which was acquired with the
// specified token, for maxDuration longer.  It returns nil if the task is still owned and its
// deadline was extended, or an SQL error, or ErrInvalidToken.
func ExtendTaskDeadline(conn *pgxpool.Conn, taskID TaskID, token PerformanceToken, maxDuration time.Duration) error {
	ctx := context.Background()
	row := conn.QueryRow(ctx, `SELECT * FROM extend_task_deadline($1, $2, $3)`, taskID, token, maxDuration)
	var res sql.NullBool
	err := row.Scan(&res)
	if err != nil {
		return fmt.Errorf("extend_task_deadline: %w", err)
	}

	if !res.Valid || !res.Bool {
		return fmt.Errorf("extend task %s token %s for %s: %w", taskID, token, maxDuration, ErrInvalidToken)
	}
	return nil
}

// ReturnTask returns taskId which was acquired using the specified performanceToken, giving it
// resultStatus and resultStatusCode.  It returns ErrInvalidToken if the performanceToken is
// invalid; this happens when ReturnTask is called after its deadline expires, or due to a logic
// error.
func ReturnTask(conn *pgxpool.Conn, taskID TaskID, token PerformanceToken, resultStatus string, resultStatusCode TaskStatusCodeValue) error {
	ctx := context.Background()
	row := conn.QueryRow(ctx, `SELECT return_task($1, $2, $3, $4)`, taskID, token, resultStatus, resultStatusCode)
	var res int
	err := row.Scan(&res)
	if err != nil {
		return fmt.Errorf("return_task: %w", err)
	}

	if res != 1 {
		return fmt.Errorf("return task %s token %s with (%s, %s): %w",
			taskID, token, resultStatus, resultStatusCode, ErrInvalidToken)
	}

	return nil
}

type waitResult struct {
	status     string
	statusCode TaskStatusCodeValue
	err        error
}

// TaskWaiter is used to wait for tasks.  It is an object not a function to prevent race
// conditions by starting to wait before beginning to act.  It owns a connection to the database
// and should not be copied.
type TaskDBWaiter struct {
	taskID     TaskID
	cancelFunc context.CancelFunc
	done       chan struct{}
	result     *waitResult
}

// NewWaiter returns TaskWaiter to wait for id on conn.  conn is owned by the returned
// TaskWaiter until the waiter is done or cancelled.
func NewWaiter(ctx context.Context, conn *pgxpool.Conn, taskID TaskID) (*TaskDBWaiter, error) {
	defer func() {
		if conn != nil {
			conn.Release()
		}
	}()
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("start waiting for %s: %w", taskID, err)
	}
	defer func() {
		if tx != nil {
			if err := tx.Rollback(ctx); err != nil {
				logging.
					FromContext(ctx).
					WithFields(logging.Fields{"taskID": taskID, "error": err}).
					Error("cannot roll back listen tx after error")
			}
		}
	}()

	row := tx.QueryRow(ctx, `SELECT notify_channel_after, status_code, status FROM tasks WHERE id=$1 FOR SHARE`, taskID)

	var (
		notifyChannel string
		status        sql.NullString
		statusCode    TaskStatusCodeValue = TaskInvalid
	)
	if err = row.Scan(&notifyChannel, &statusCode, &status); err != nil {
		return nil, fmt.Errorf("check task %s to listen: %w", taskID, err)
	}
	if notifyChannel == "" {
		return nil, fmt.Errorf("cannot wait for task %s: %w", taskID, ErrNoNotifyChannel)
	}
	if statusCode != TaskInProgress && statusCode != TaskPending {
		if !status.Valid {
			status.String = ""
		}
		return &TaskDBWaiter{
			taskID: taskID,
			result: &waitResult{
				status:     status.String,
				statusCode: statusCode,
				err:        nil,
			},
		}, nil
	}

	if _, err = conn.Exec(ctx, "LISTEN "+pgx.Identifier{notifyChannel}.Sanitize()); err != nil {
		return nil, fmt.Errorf("listen for %s: %w", notifyChannel, err)
	}
	err = tx.Commit(ctx)
	tx = nil
	if err != nil {
		return nil, fmt.Errorf("commit listen for %s tx: %w", taskID, err)
	}

	// Wait for task, report when done.
	waitCtx, cancelFunc := context.WithCancel(ctx)

	doneCh := make(chan struct{})
	ret := &TaskDBWaiter{
		taskID:     taskID,
		done:       doneCh,
		cancelFunc: cancelFunc,
		result:     nil,
	}

	go func(conn *pgxpool.Conn) {
		defer conn.Release()
		_, err := conn.Conn().WaitForNotification(waitCtx)
		if err != nil {
			ret.result = &waitResult{
				err:        fmt.Errorf("wait for notification %s: %w", notifyChannel, err),
				statusCode: TaskInvalid,
			}
			return
		}
		row := conn.QueryRow(waitCtx, `SELECT status, status_code FROM tasks WHERE id=$1`, taskID)
		var (
			status     sql.NullString
			statusCode TaskStatusCodeValue = TaskInvalid
		)
		err = row.Scan(&status, &statusCode)
		if err != nil {
			err = fmt.Errorf("query status for task %s: %w", taskID, err)
		}
		if !status.Valid {
			status.String = ""
		}
		ret.result = &waitResult{
			status:     status.String,
			statusCode: statusCode,
			err:        err,
		}
		close(ret.done)
	}(conn)

	conn = nil

	return ret, nil
}

// Wait waits for the task to finish or the waiter to be cancelled and returns the task status
// and status code.  It may safely be called from multiple goroutines.
func (tw *TaskDBWaiter) Wait() (string, TaskStatusCodeValue, error) {
	if tw.done != nil {
		<-tw.done
	}
	return tw.result.status, tw.result.statusCode, tw.result.err
}

// Cancel cancels waiting.
func (tw *TaskDBWaiter) Cancel() {
	tw.cancelFunc()
}

// taskWithNewIterator is a pgx.CopyFromSource iterator that passes each task along with a
// 'new' copy status.
type taskWithNewIterator struct {
	tasks []TaskID
	idx   int
}

func (it *taskWithNewIterator) Next() bool {
	it.idx++
	return it.idx < len(it.tasks)
}

func (it *taskWithNewIterator) Values() ([]interface{}, error) {
	return []interface{}{it.tasks[it.idx], "new"}, nil
}

func (it *taskWithNewIterator) Err() error {
	return nil
}

func makeTaskWithNewIterator(tasks []TaskID) *taskWithNewIterator {
	return &taskWithNewIterator{tasks, -1}
}

// DeleteTasks deletes taskIds, removing dependencies and deleting (effectively recursively) any
// tasks that are left with no dependencies.  It creates a temporary table on tx, so ideally
// close the transaction shortly after.  The effect is easiest to analyze when all deleted tasks
// have been either completed or been aborted.
func DeleteTasks(ctx context.Context, tx pgx.Tx, taskIds []TaskID) error {
	uniqueID, err := nanoid.Nanoid()
	if err != nil {
		return fmt.Errorf("generate random component for table name: %w", err)
	}
	tableName := fmt.Sprintf("delete_tasks_%s", uniqueID)
	if _, err = tx.Exec(
		ctx,
		fmt.Sprintf(`CREATE TEMP TABLE "%s" (id VARCHAR(64), mark tasks_recurse_value NOT NULL) ON COMMIT DROP`, tableName),
	); err != nil {
		return fmt.Errorf("create temp work table %s: %w", tableName, err)
	}

	if _, err = tx.CopyFrom(ctx, pgx.Identifier{tableName}, []string{"id", "mark"}, makeTaskWithNewIterator(taskIds)); err != nil {
		return fmt.Errorf("COPY: %w", err)
	}
	_, err = tx.Exec(ctx, `SELECT delete_tasks($1)`, tableName)
	return err
}
