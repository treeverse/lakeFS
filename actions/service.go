package actions

//go:generate mockgen -package=mock -destination=mock/mock_actions.go github.com/treeverse/lakefs/actions Source,OutputWriter

import (
	"context"
	"errors"
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
	RunID  string
	Action *Action
	HookID string
	Hook   Hook
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

type RunResult struct {
	RunID     string
	BranchID  string
	OnRef     string
	Action    *Action
	StartTime time.Time
	EndTime   time.Time
	Passed    bool
}

type TaskResultIter interface {
	Next() bool
	Value() TaskResult
	// SeekGE seeks by start-time
	SeekGE(time.Time)
	Err() error
	Close()
}

type RunResultIter interface {
	Next() bool
	Value() RunResult
	// SeekGE seeks by start-time
	SeekGE(time.Time)
	Err() error
	Close()
}

var (
	ErrNotFound = errors.New("not found")
)

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
	return s.runTasks(ctx, tasks, event, deps)
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

func (s *Service) runTasks(ctx context.Context, hooks []*Task, event Event, deps Deps) error {
	var g multierror.Group
	for _, h := range hooks {
		hh := h // pinning
		g.Go(func() error {
			return hh.Hook.Run(ctx, event, &HookOutputWriter{
				RunID:      hh.RunID,
				ActionName: hh.Action.Name,
				HookID:     hh.HookID,
				Writer:     deps.Output,
			})
		})
	}
	return g.Wait().ErrorOrNil()
}

func NewRunID(t time.Time) string {
	uid := strings.ReplaceAll(uuid.New().String(), "-", "")
	runID := t.UTC().Format(time.RFC3339) + "_" + uid
	return runID
}
