package actions

//go:generate mockgen -package=mock -destination=mock/mock_actions.go github.com/treeverse/lakefs/actions Source,OutputWriter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/logging"

	"github.com/hashicorp/go-multierror"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/graveler"
)

type Service struct {
	DB     db.Database
	Source Source
	Writer OutputWriter
}

type Task struct {
	RunID     string
	HookRunID string
	Action    *Action
	HookID    string
	Hook      Hook
	Err       error
	StartTime time.Time
	EndTime   time.Time
}

type RunResult struct {
	RunID     string    `db:"run_id"`
	BranchID  string    `db:"branch_id"`
	SourceRef string    `db:"source_ref"`
	EventType string    `db:"event_type"`
	StartTime time.Time `db:"start_time"`
	EndTime   time.Time `db:"end_time"`
	Passed    bool      `db:"passed"`
	CommitID  string    `db:"commit_id"`
}

type TaskResult struct {
	RunID      string    `db:"run_id"`
	HookRunID  string    `db:"hook_run_id"`
	HookID     string    `db:"hook_id"`
	ActionName string    `db:"action_name"`
	StartTime  time.Time `db:"start_time"`
	EndTime    time.Time `db:"end_time"`
	Passed     bool      `db:"passed"`
}

func (r *TaskResult) LogPath() string {
	return FormatHookOutputPath(r.RunID, r.ActionName, r.HookID)
}

type RunResultIterator interface {
	Next() bool
	Value() *RunResult
	Err() error
	Close()
}

type TaskResultIterator interface {
	Next() bool
	Value() *TaskResult
	Err() error
	Close()
}

const defaultFetchSize = 1024

var ErrNotFound = errors.New("not found")

func NewService(db db.Database, source Source, writer OutputWriter) *Service {
	return &Service{
		DB:     db,
		Source: source,
		Writer: writer,
	}
}

// Run load and run actions based on the event information
func (s *Service) Run(ctx context.Context, record graveler.HookRecord) error {
	// load relevant actions
	spec := MatchSpec{
		EventType: record.EventType,
		BranchID:  record.BranchID,
	}
	logging.Default().WithField("record", record).WithField("spec", spec).Info("Filtering actions")
	actions, err := s.loadMatchedActions(ctx, record, spec)
	if err != nil || len(actions) == 0 {
		return err
	}

	// allocate and run hooks
	tasks, err := s.allocateTasks(record.RunID, actions)
	if err != nil {
		return err
	}

	runErr := s.runTasks(ctx, record, tasks)

	// keep results before returning an error (if any)
	err = s.insertRunInformation(ctx, record, tasks)
	if err != nil {
		return err
	}

	return runErr
}

func (s *Service) loadMatchedActions(ctx context.Context, record graveler.HookRecord, spec MatchSpec) ([]*Action, error) {
	actions, err := LoadActions(ctx, s.Source, record)
	if err != nil {
		return nil, err
	}
	return MatchedActions(actions, spec)
}

func (s *Service) allocateTasks(runID string, actions []*Action) ([][]*Task, error) {
	var tasks [][]*Task
	for _, action := range actions {
		var actionTasks []*Task
		for _, hook := range action.Hooks {
			h, err := NewHook(hook, action)
			if err != nil {
				return nil, err
			}
			task := &Task{
				RunID:     runID,
				HookRunID: graveler.NewRunID(),
				Action:    action,
				HookID:    hook.ID,
				Hook:      h,
			}
			// append new task or chain to the last one based on the current action
			actionTasks = append(actionTasks, task)
		}
		if len(actionTasks) > 0 {
			tasks = append(tasks, actionTasks)
		}
	}
	return tasks, nil
}

func (s *Service) runTasks(ctx context.Context, record graveler.HookRecord, tasks [][]*Task) error {
	var g multierror.Group
	for _, actionTasks := range tasks {
		actionTasks := actionTasks // pin
		g.Go(func() error {
			for _, task := range actionTasks {
				hookOutputWriter := &HookOutputWriter{
					Writer:           s.Writer,
					StorageNamespace: record.StorageNamespace.String(),
					RunID:            task.RunID,
					HookRunID:        task.HookRunID,
					ActionName:       task.Action.Name,
					HookID:           task.HookID,
				}
				task.StartTime = time.Now()
				task.Err = task.Hook.Run(ctx, record, hookOutputWriter)
				task.EndTime = time.Now()

				if task.Err != nil {
					// wrap error with more information and return
					task.Err = fmt.Errorf("run '%s' failed on action '%s' hook '%s': %w",
						task.RunID, task.Action.Name, task.HookID, task.Err)
					return task.Err
				}
			}
			return nil
		})
	}
	return g.Wait().ErrorOrNil()
}

func (s *Service) insertRunInformation(ctx context.Context, record graveler.HookRecord, tasks [][]*Task) error {
	if len(tasks) == 0 {
		return nil
	}
	// collect run information based on tasks
	runStartTime, runEndTime, runPassed := runInformationBasedOnTasks(tasks)

	// save run and tasks information
	_, err := s.DB.Transact(func(tx db.Tx) (interface{}, error) {
		// insert run information
		_, err := tx.Exec(`INSERT INTO actions_runs(repository_id, run_id, event_type, start_time, end_time, branch_id, source_ref, commit_id, passed)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			record.RepositoryID, record.RunID, record.EventType, runStartTime, runEndTime, record.BranchID, record.SourceRef, record.CommitID, runPassed)
		if err != nil {
			return nil, fmt.Errorf("insert run information: %w", err)
		}

		// insert each task information
		for _, actionTasks := range tasks {
			for _, task := range actionTasks {
				if task.StartTime.IsZero() {
					break
				}
				taskPassed := task.Err == nil
				_, err = tx.Exec(`INSERT INTO actions_run_hooks(repository_id, run_id, hook_run_id, action_name, hook_id, start_time, end_time, passed)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
					record.RepositoryID, task.RunID, task.HookRunID, task.Action.Name, task.HookID, task.StartTime, task.EndTime, taskPassed)
				if err != nil {
					return nil, fmt.Errorf("insert run hook information (%s %s): %w", task.Action.Name, task.HookID, err)
				}
			}
		}
		return nil, nil
	}, db.WithContext(ctx))
	return err
}

func runInformationBasedOnTasks(tasks [][]*Task) (startTime time.Time, endTime time.Time, passed bool) {
	passed = true
	for _, actionTasks := range tasks {
		for _, task := range actionTasks {
			// skip scan when task didn't run
			if task.StartTime.IsZero() {
				break
			}
			// keep min start time
			if startTime.IsZero() || task.StartTime.Before(startTime) {
				startTime = task.StartTime
			}
			// keep max end time
			if endTime.IsZero() || task.EndTime.After(endTime) {
				endTime = task.EndTime
			}
			// did we failed
			if passed && task.Err != nil {
				passed = false
			}
		}
	}
	return
}

func (s *Service) UpdateCommitID(ctx context.Context, repositoryID string, runID string, commitID string) error {
	_, err := s.DB.Transact(func(tx db.Tx) (interface{}, error) {
		_, err := tx.Exec(`UPDATE actions_runs SET commit_id=$3 WHERE repository_id=$1 AND run_id=$2`,
			repositoryID, runID, commitID)
		if err != nil {
			return nil, fmt.Errorf("update run commit_id: %w", err)
		}
		return nil, nil
	}, db.WithContext(ctx))
	return err
}

func (s *Service) GetRunResult(ctx context.Context, repositoryID string, runID string) (*RunResult, error) {
	res, err := s.DB.Transact(func(tx db.Tx) (interface{}, error) {
		result := &RunResult{
			RunID: runID,
		}
		err := tx.Get(result, `SELECT event_type, branch_id, source_ref, start_time, end_time, passed, commit_id
			FROM actions_runs
			WHERE repository_id=$1 AND run_id=$2`,
			repositoryID, runID)
		if err != nil {
			return nil, fmt.Errorf("get run result: %w", err)
		}
		return result, nil
	}, db.WithContext(ctx), db.ReadOnly())
	if errors.Is(err, db.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return res.(*RunResult), nil
}

func (s *Service) GetTaskResult(ctx context.Context, repositoryID string, runID string, hookRunID string) (*TaskResult, error) {
	res, err := s.DB.Transact(func(tx db.Tx) (interface{}, error) {
		result := &TaskResult{
			RunID:     runID,
			HookRunID: hookRunID,
		}
		err := tx.Get(result, `SELECT hook_id, action_name, start_time, end_time, passed
			FROM actions_run_hooks 
			WHERE repository_id=$1 AND run_id=$2 AND hook_run_id=$3`,
			repositoryID, runID, hookRunID)
		if err != nil {
			return nil, fmt.Errorf("get task result: %w", err)
		}
		return result, nil
	}, db.WithContext(ctx), db.ReadOnly())
	if errors.Is(err, db.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return res.(*TaskResult), nil
}

func (s *Service) ListRunResults(ctx context.Context, repositoryID string, branchID *string, after string) (RunResultIterator, error) {
	return NewDBRunResultIterator(ctx, s.DB, defaultFetchSize, repositoryID, branchID, nil, after), nil
}

func (s *Service) ListCommitRunResults(ctx context.Context, repositoryID string, commitID string) (RunResultIterator, error) {
	return NewDBRunResultIterator(ctx, s.DB, defaultFetchSize, repositoryID, nil, &commitID, ""), nil
}

func (s *Service) ListRunTaskResults(ctx context.Context, repositoryID string, runID string, after string) (TaskResultIterator, error) {
	return NewDBTaskResultIterator(ctx, s.DB, defaultFetchSize, repositoryID, runID, after), nil
}

func (s *Service) PreCommitHook(ctx context.Context, record graveler.HookRecord) error {
	return s.Run(ctx, record)
}

func (s *Service) PostCommitHook(ctx context.Context, record graveler.HookRecord) error {
	// update pre-commit with commit ID if needed
	err := s.UpdateCommitID(ctx, record.RepositoryID.String(), record.PreRunID, record.CommitID.String())
	if err != nil {
		return err
	}
	// post commit actions will be enabled here
	return nil
}

func (s *Service) PreMergeHook(ctx context.Context, record graveler.HookRecord) error {
	return s.Run(ctx, record)
}

func (s *Service) PostMergeHook(ctx context.Context, record graveler.HookRecord) error {
	// update pre-merge with commit ID if needed
	err := s.UpdateCommitID(ctx, record.RepositoryID.String(), record.PreRunID, record.CommitID.String())
	if err != nil {
		return err
	}
	// post merge actions will be enabled here
	return nil
}
