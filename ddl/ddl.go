package ddl

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jmoiron/sqlx"
)

type TaskId string

type ActorId string

type PerformanceToken pgtype.UUID

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
	addBytes(4)
	res.WriteString("-")
	addBytes(2)
	res.WriteString("-")
	addBytes(2)
	res.WriteString("-")
	addBytes(2)
	res.WriteString("-")
	addBytes(6)
	return res.String()
}

type TaskStatusCodeValue string

const (
	// TASK_PENDING indicates a task is waiting for an actor to perform it (new or being
	// retried)
	TASK_PENDING TaskStatusCodeValue = "pending"
	// IN_PROGRESS indicates a task is being performed by an actor.
	TASK_IN_PROGRESS TaskStatusCodeValue = "in-progress"
	// ABORTED indicates an actor has aborted this task with message, will not be reissued
	TASK_ABORTED TaskStatusCodeValue = "aborted"
	// TASK_COMPLETED indicates an actor has completed this task with message, will not reissued
	TASK_COMPLETED TaskStatusCodeValue = "completed"
	// TASK_INVALID is used by the API to report errors
	TASK_INVALID TaskStatusCodeValue = "[invalid]"
)

// TaskData is a row in table "tasks".  It describes a task to perform.
type TaskData struct {
	Id     TaskId `db:"task_id"`
	Action string `db:"action"`
	// Body is JSON-formatted
	Body              *string           `db:"body"`
	Status            *string           `db:"status"`
	StatusCode        string            `db:"status_code"`
	NumTries          int               `db:"num_tries"`
	MaxTries          *int              `db:"max_tries"`
	ActorId           ActorId           `db:"actor_id"`
	ActionDeadline    *time.Time        `db:"action_deadline"`
	PerformanceToken  *PerformanceToken `db:"performance_token"`
	FinishChannelName *string           `db:"finish_channel"`
}

// TaskDependencyData is a row in table "task_dependencies".  It describes that task Run must
// occur after task After succeeds.
type TaskDependencyData struct {
	After TaskId
	Run   TaskId
}

// OwnedTaskData is a row returned from "SELECT * FROM own_tasks(...)".
type OwnedTaskData struct {
	Id    TaskId           `db:"task_id"`
	Token PerformanceToken `db:"token"`
	Body  *string
}

// OwnTasks owns for actor and returns up to maxTasks tasks for performing any of actions.
func OwnTasks(conn *sqlx.DB, actor ActorId, maxTasks int, actions []string, maxDuration *time.Duration) ([]OwnedTaskData, error) {
	// Use sqlx.In to expand slice actions
	query, args, err := sqlx.In(`SELECT * FROM own_tasks(?, ARRAY[?], ?, ?)`, maxTasks, actions, actor, maxDuration)
	if err != nil {
		return nil, fmt.Errorf("expand own tasks query: %s", err)
	}
	query = conn.Rebind(query)
	rows, err := conn.Queryx(query, args...)
	if err != nil {
		return nil, fmt.Errorf("try to own tasks: %w", err)
	}
	tasks := make([]OwnedTaskData, 0, maxTasks)
	for rows.Next() {
		var task OwnedTaskData
		if err = rows.StructScan(&task); err != nil {
			return nil, fmt.Errorf("failed to scan row %+v: %w", rows, err)
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

var InvalidTokenError = errors.New("performance token invalid (action may have exceeded deadline)")

// ReturnTask returns taskId which was acquired using the specified performanceToken, giving it
// resultStatus and resultStatusCode.  It returns InvalidTokenError if the performanceToken is
// invalid; this happens when ReturnTask is called after its deadline expires, or due to a logic
// error.
func ReturnTask(conn *sqlx.DB, taskId TaskId, token PerformanceToken, resultStatus string, resultStatusCode TaskStatusCodeValue) error {
	var res int
	query, args, err := sqlx.In(`SELECT return_task(?, ?, ?, ?)`, taskId, token, resultStatus, resultStatusCode)
	query = conn.Rebind(query)
	err = conn.Get(&res, query, args...)
	if err != nil {
		return fmt.Errorf("return_task: %w", err)
	}

	if res != 1 {
		return InvalidTokenError
	}

	return nil
}

// WaitForTask blocks until taskId ends, and returns its result status and status code.  It
// needs a pgx.Conn -- *not* a sqlx.Conn -- because it depends on PostgreSQL specific features.
func WaitForTask(ctx context.Context, conn *pgx.Conn, taskId TaskId) (resultStatus string, resultStatusCode TaskStatusCodeValue, err error) {
	row := conn.QueryRow(ctx, `SELECT finish_channel, status_code FROM tasks WHERE id=$1`, taskId)
	var (
		finishChannel string
		statusCode    TaskStatusCodeValue
		status        string
	)
	if err = row.Scan(&finishChannel, &statusCode); err != nil {
		return "", TASK_INVALID, fmt.Errorf("check task %s to listen: %w", taskId, err)
	}
	if statusCode != TASK_IN_PROGRESS && statusCode != TASK_PENDING {
		return "", statusCode, fmt.Errorf("task %s already in status %s", taskId, statusCode)
	}

	if _, err = conn.Exec(ctx, "LISTEN "+pgx.Identifier{finishChannel}.Sanitize()); err != nil {
		return "", TASK_INVALID, fmt.Errorf("listen for %s: %w", finishChannel, err)
	}

	_, err = conn.WaitForNotification(ctx)
	if err != nil {
		return "", TASK_INVALID, fmt.Errorf("wait for notification %s: %w", finishChannel, err)
	}

	row = conn.QueryRow(ctx, `SELECT status, status_code FROM tasks WHERE id=$1`, taskId)
	status = ""
	statusCode = TASK_INVALID
	err = row.Scan(&status, &statusCode)
	return status, statusCode, err
}
