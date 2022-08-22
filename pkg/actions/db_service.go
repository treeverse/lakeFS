package actions

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/hashicorp/go-multierror"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/xid"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	kvpg "github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/version"
	"google.golang.org/protobuf/proto"
)

//nolint:gochecknoinits
func init() {
	kvpg.RegisterMigrate(packageName, Migrate, []string{"actions_run_hooks", "actions_runs"})
}

const (
	runWorkers       = 10
	taskWorkers      = 10
	migrateQueueSize = 100
)

type migrateTask struct {
	oldID string
	newID string
}

var encoder kv.SafeEncoder

type runResultMigrate struct {
	RunResult
	RepoID string `db:"repository_id" json:"repository_id"`
}

type taskResultMigrate struct {
	TaskResult
	RepoID string `db:"repository_id" json:"repository_id"`
}

// runWorker writing run data to fd
func runWorker(jobChan <-chan *runResultMigrate) error {
	for m := range jobChan {
		pr := protoFromRunResult(&m.RunResult)
		value, err := proto.Marshal(pr)
		if err != nil {
			return err
		}

		key := RunPath(m.RepoID, m.RunResult.RunID)
		if err = encoder.Encode(kv.Entry{
			PartitionKey: []byte(PartitionKey),
			Key:          key,
			Value:        value,
		}); err != nil {
			return err
		}

		// Add secondary keys branch
		if m.BranchID != "" {
			secKey := RunByBranchPath(m.RepoID, m.BranchID, m.RunResult.RunID)
			sec := kv.SecondaryIndex{PrimaryKey: key}
			data, err := proto.Marshal(&sec)
			if err != nil {
				return err
			}

			if err = encoder.Encode(kv.Entry{
				PartitionKey: []byte(PartitionKey),
				Key:          secKey,
				Value:        data,
			}); err != nil {
				return err
			}
		}

		// Add secondary keys commit
		if m.CommitID != "" {
			secKey := RunByCommitPath(m.RepoID, m.CommitID, m.RunResult.RunID)
			sec := kv.SecondaryIndex{PrimaryKey: key}
			data, err := proto.Marshal(&sec)
			if err != nil {
				return err
			}
			if err = encoder.Encode(kv.Entry{
				PartitionKey: []byte(PartitionKey),
				Key:          secKey,
				Value:        data,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// tasksWorker dispatch taskWorker jobs per run
func tasksWorker(ctx context.Context, d *pgxpool.Pool, jobChan <-chan *migrateTask) error {
	tasksChan := make(chan *taskResultMigrate, migrateQueueSize)
	var g multierror.Group
	for i := 0; i < taskWorkers; i++ {
		g.Go(func() error {
			return taskWorker(tasksChan)
		})
	}
	for t := range jobChan {
		tasksRows, err := d.Query(ctx, "SELECT * FROM actions_run_hooks where run_id=$1", t.oldID)
		if err != nil {
			return err
		}
		taskScanner := pgxscan.NewRowScanner(tasksRows)
		for tasksRows.Next() {
			tm := new(taskResultMigrate)
			err = taskScanner.Scan(tm)
			if err != nil {
				return err
			}
			tm.RunID = t.newID
			tasksChan <- tm
		}
		tasksRows.Close()
	}
	close(tasksChan)

	return g.Wait().ErrorOrNil()
}

// taskWorker writing task data to fd
func taskWorker(jobChan <-chan *taskResultMigrate) error {
	for t := range jobChan {
		taskPr := protoFromTaskResult(&t.TaskResult)
		taskValue, err := proto.Marshal(taskPr)
		if err != nil {
			return err
		}
		taskKey := []byte(kv.FormatPath(TasksPath(t.RepoID, t.RunID), t.HookRunID))
		if err = encoder.Encode(kv.Entry{
			PartitionKey: []byte(PartitionKey),
			Key:          taskKey,
			Value:        taskValue,
		}); err != nil {
			return err
		}
	}
	return nil
}

func Migrate(ctx context.Context, d *pgxpool.Pool, writer io.Writer) error {
	encoder = kv.SafeEncoder{
		Je: json.NewEncoder(writer),
		Mu: sync.Mutex{},
	}
	// Create header
	if err := encoder.Encode(kv.Header{
		LakeFSVersion:   version.Version,
		PackageName:     packageName,
		DBSchemaVersion: kv.InitialMigrateVersion,
		CreatedAt:       time.Now().UTC(),
	}); err != nil {
		return err
	}

	runChan := make(chan *runResultMigrate, migrateQueueSize)
	tasksChan := make(chan *migrateTask, migrateQueueSize)
	var g multierror.Group
	for i := 0; i < runWorkers; i++ {
		// Run worker reads runs from DB and writes respected entries to fd
		g.Go(func() error {
			return runWorker(runChan)
		})
		// tasks worker scans tasks per run ID and starts the task go routines which write the respected entries to fd
		g.Go(func() error {
			return tasksWorker(ctx, d, tasksChan)
		})
	}

	rows, err := d.Query(ctx, "SELECT * FROM actions_runs ORDER BY run_id DESC")
	if err != nil {
		return err
	}
	defer rows.Close()
	rowScanner := pgxscan.NewRowScanner(rows)
	for rows.Next() {
		m := new(runResultMigrate)
		err = rowScanner.Scan(m)
		if err != nil {
			close(tasksChan)
			close(runChan)
			return err
		}
		runTime, err := time.Parse(graveler.RunIDTimeLayout, m.RunResult.RunID[:len(graveler.RunIDTimeLayout)])
		if err != nil {
			close(tasksChan)
			close(runChan)
			return err
		}
		newRunID := newRunIDFromTime(runTime)
		oldRunID := m.RunResult.RunID
		m.RunResult.RunID = newRunID
		runChan <- m
		t := &migrateTask{
			oldID: oldRunID,
			newID: newRunID,
		}
		tasksChan <- t
	}
	close(tasksChan)
	close(runChan)
	err = g.Wait().ErrorOrNil()
	if err != nil {
		return err
	}
	return nil
}

func newRunIDFromTime(t time.Time) string {
	tm := time.Unix(graveler.UnixYear3000-t.Unix(), 0).UTC()
	return xid.NewWithTime(tm).String()
}
