package actions

//go:generate mockgen -package=mock -destination=mock/mock_actions.go github.com/treeverse/lakefs/actions Source,OutputWriter

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/treeverse/lakefs/db"
)

type Service struct {
	DB db.Database
}

type Task struct {
	RunID     string
	Action    *Action
	HookID    string
	Hook      Hook
	Err       error
	StartTime time.Time
	EndTime   time.Time
}

type RunResult struct {
	RunID      string
	BranchID   string
	OnRef      string
	ActionName string
	StartTime  time.Time
	EndTime    time.Time
	Passed     bool
}

type TaskResult struct {
	RunID      string
	HookID     string
	HookType   string
	ActionName string
	StartTime  time.Time
	EndTime    time.Time
	Passed     bool
}

func New(db db.Database) *Service {
	return &Service{
		DB: db,
	}
}
func (s *Service) Run(ctx context.Context, event Event, deps Deps) error {
	// load relevant actions
	actions, err := s.loadMatchedActions(ctx, deps.Source, MatchSpec{EventType: event.EventType, Branch: event.BranchID})
	if err != nil || len(actions) == 0 {
		return err
	}

	// allocate and run hooks
	runID := NewRunID(event.EventTime)
	tasks, err := s.allocateTasks(runID, actions)
	if err != nil {
		return nil
	}

	runErr := s.runTasks(ctx, tasks, event, deps)
	endTime := time.Now()

	// write results and return multi error
	err = s.insertRunInformation(ctx, event, tasks, endTime, runErr)
	if err != nil {
		return err
	}
	return runErr
}

func (s *Service) loadMatchedActions(ctx context.Context, source Source, spec MatchSpec) ([]*Action, error) {
	if source == nil {
		return nil, nil
	}
	actions, err := LoadActions(ctx, source)
	if err != nil {
		return nil, err
	}
	return MatchedActions(actions, spec)
}

func (s *Service) allocateTasks(runID string, actions []*Action) ([]*Task, error) {
	var tasks []*Task
	for _, action := range actions {
		for _, hook := range action.Hooks {
			h, err := NewHook(hook, action)
			if err != nil {
				return nil, err
			}
			tasks = append(tasks, &Task{
				RunID:  runID,
				Action: action,
				HookID: hook.ID,
				Hook:   h,
			})
		}
	}
	return tasks, nil
}

func (s *Service) runTasks(ctx context.Context, tasks []*Task, event Event, deps Deps) error {
	var g multierror.Group
	for _, task := range tasks {
		task := task // pin
		g.Go(func() error {
			task.StartTime = time.Now()
			task.Err = task.Hook.Run(ctx, event, &HookOutputWriter{
				RunID:      task.RunID,
				ActionName: task.Action.Name,
				HookID:     task.HookID,
				Writer:     deps.Output,
			})
			task.EndTime = time.Now()
			return task.Err
		})
	}
	return g.Wait().ErrorOrNil()
}

func (s *Service) insertRunInformation(ctx context.Context, event Event, tasks []*Task, endTime time.Time, runErr error) error {
	_, err := s.DB.Transact(func(tx db.Tx) (interface{}, error) {
		var err error
		// insert run information
		runPassed := runErr == nil
		_, err = tx.Exec(`INSERT INTO actions_runs(repository_id, run_id, event_type, start_time, end_time, branch_id, source_ref, commit_id, passed)
			VALUES ($1,$2,$3,$4,$5,$6,$7,'',$8)`,
			event.RepositoryID, event.RunID.String(), event.EventType, event.EventTime, endTime, event.BranchID, event.SourceRef, runPassed)
		if err != nil {
			return nil, fmt.Errorf("insert run information: %w", err)
		}

		// insert each task information
		for _, task := range tasks {
			taskPassed := runErr == nil
			_, err = tx.Exec(`INSERT INTO actions_run_hooks(repository_id, run_id, event_type, action_name, hook_id, start_time, end_time, passed)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
				event.RepositoryID, event.RunID.String(), event.EventType, task.Action.Name, task.HookID, task.StartTime, task.EndTime, taskPassed)
			if err != nil {
				return nil, fmt.Errorf("insert run hook information (%s %s): %w", task.Action.Name, task.HookID, err)
			}
		}
		return nil, nil
	}, db.WithContext(ctx))
	return err
}

func NewRunID(t time.Time) string {
	uid := strings.ReplaceAll(uuid.New().String(), "-", "")
	runID := t.UTC().Format(time.RFC3339) + "_" + uid
	return runID
}

func (s *Service) UpdateCommitID(ctx context.Context, repositoryID string, runID uuid.UUID, eventType EventType, commitID string) error {
	_, err := s.DB.Transact(func(tx db.Tx) (interface{}, error) {
		_, err := tx.Exec(`UPDATE actions_runs SET commit_id=$4 WHERE repository_id=$1 AND run_id=$2 AND event_type=$3`,
			repositoryID, runID, string(eventType), commitID)
		if err != nil {
			return nil, fmt.Errorf("update run commit_id: %w", err)
		}
		return nil, nil
	}, db.WithContext(ctx))
	return err
}
