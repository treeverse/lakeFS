package testutil

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/treeverse/lakefs/parade"
)

type Wrapper struct {
	TB testing.TB
	DB *sqlx.DB
}

func (w Wrapper) Prefix(s string) string {
	return fmt.Sprintf("%s.%s", w.TB.Name(), s)
}

func (w Wrapper) Strip(s string) string {
	return strings.TrimPrefix(s, w.TB.Name()+".")
}

func (w Wrapper) PrefixTask(id parade.TaskID) parade.TaskID {
	return parade.TaskID(w.Prefix(string(id)))
}

func (w Wrapper) StripTask(id parade.TaskID) parade.TaskID {
	return parade.TaskID(w.Strip(string(id)))
}

func (w Wrapper) PrefixActor(actor parade.ActorID) parade.ActorID {
	return parade.ActorID(w.Prefix(string(actor)))
}

func (w Wrapper) StripActor(actor parade.TaskID) parade.ActorID {
	return parade.ActorID(w.Strip(string(actor)))
}

func (w Wrapper) InsertTasks(tasks []parade.TaskData) func() {
	w.TB.Helper()
	ctx := context.Background()
	sqlConn, err := w.DB.Conn(ctx)
	if err != nil {
		w.TB.Fatalf("sqlx.DB.Conn: %s", err)
	}
	defer sqlConn.Close()

	conn, err := stdlib.AcquireConn(w.DB.DB)
	if err != nil {
		w.TB.Fatalf("stdlib.AcquireConn: %s", err)
	}
	defer stdlib.ReleaseConn(w.DB.DB, conn)

	prefixedTasks := make([]parade.TaskData, len(tasks))
	for i := 0; i < len(tasks); i++ {
		copy := tasks[i]
		copy.ID = w.PrefixTask(copy.ID)
		copy.Action = w.Prefix(copy.Action)
		copy.ActorID = w.PrefixActor(copy.ActorID)
		if copy.StatusCode == "" {
			copy.StatusCode = "pending"
		}
		toSignal := make([]parade.TaskID, len(copy.ToSignal))
		for j := 0; j < len(toSignal); j++ {
			toSignal[j] = w.PrefixTask(copy.ToSignal[j])
		}
		copy.ToSignal = toSignal
		prefixedTasks[i] = copy
	}
	err = parade.InsertTasks(ctx, conn, &parade.TaskDataIterator{Data: prefixedTasks})
	if err != nil {
		w.TB.Fatalf("InsertTasks: %s", err)
	}

	// Create cleanup callback.  Compute the ids now, tasks may change later.
	ids := make([]parade.TaskID, 0, len(tasks))

	for _, task := range tasks {
		ids = append(ids, task.ID)
	}

	return func() { w.DeleteTasks(ids) }
}

func (w Wrapper) DeleteTasks(ids []parade.TaskID) error {
	prefixedIds := make([]parade.TaskID, len(ids))
	for i := 0; i < len(ids); i++ {
		prefixedIds[i] = w.PrefixTask(ids[i])
	}
	ctx := context.Background()
	conn, err := stdlib.AcquireConn(w.DB.DB)
	if err != nil {
		w.TB.Fatalf("stdlib.AcquireConn: %s", err)
	}
	defer stdlib.ReleaseConn(w.DB.DB, conn)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("BEGIN: %w", err)
	}
	defer func() {
		if tx != nil {
			tx.Rollback(ctx)
		}
	}()

	if err = parade.DeleteTasks(ctx, tx, prefixedIds); err != nil {
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("COMMIT: %w", err)
	}
	tx = nil
	return nil
}

func (w Wrapper) ReturnTask(taskId parade.TaskID, token parade.PerformanceToken, resultStatus string, resultStatusCode parade.TaskStatusCodeValue) error {
	return parade.ReturnTask(w.DB, w.PrefixTask(taskId), token, resultStatus, resultStatusCode)
}

func (w Wrapper) OwnTasks(actorId parade.ActorID, maxTasks int, actions []string, maxDuration *time.Duration) ([]parade.OwnedTaskData, error) {
	prefixedActions := make([]string, len(actions))
	for i, action := range actions {
		prefixedActions[i] = w.Prefix(action)
	}
	tasks, err := parade.OwnTasks(w.DB, actorId, maxTasks, prefixedActions, maxDuration)
	if tasks != nil {
		for i := 0; i < len(tasks); i++ {
			task := &tasks[i]
			task.ID = w.StripTask(task.ID)
		}
	}
	return tasks, err
}
