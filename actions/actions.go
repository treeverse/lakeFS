package actions

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/treeverse/lakefs/db"
)

type Actions struct {
	DB db.Database
}

type Task struct {
	RunID  string
	Action *Action
	HookID string
	Hook   Hook
}

func New(db db.Database) *Actions {
	return &Actions{
		DB: db,
	}
}

func (m *Actions) Run(ctx context.Context, event Event) error {
	// load relevant actions
	actions, err := m.loadMatchedActions(event.Source, MatchSpec{EventType: event.EventType, Branch: event.BranchID})
	if err != nil || len(actions) == 0 {
		return err
	}
	// allocate and run hooks
	runID := NewRunID(event.EventTime)
	tasks, err := m.allocateTasks(runID, actions)
	if err != nil {
		return nil
	}
	return m.runTasks(ctx, tasks, event)
}

func (m *Actions) loadMatchedActions(source Source, spec MatchSpec) ([]*Action, error) {
	if source == nil {
		return nil, nil
	}
	actions, err := LoadActions(source)
	if err != nil {
		return nil, err
	}
	return MatchedActions(actions, spec)
}

func (m *Actions) allocateTasks(runID string, actions []*Action) ([]*Task, error) {
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

func (m *Actions) runTasks(ctx context.Context, hooks []*Task, event Event) error {
	var g multierror.Group
	for _, h := range hooks {
		hh := h // pinning
		g.Go(func() error {
			// set hook's event to have scoped writer
			hookEvent := event
			hookEvent.Output = &HookOutputWriter{
				RunID:      hh.RunID,
				ActionName: hh.Action.Name,
				HookID:     hh.HookID,
				Writer:     event.Output,
			}
			return hh.Hook.Run(ctx, hookEvent)
		})
	}
	return g.Wait().ErrorOrNil()
}

func NewRunID(t time.Time) string {
	uid := strings.ReplaceAll(uuid.New().String(), "-", "")
	runID := t.UTC().Format(time.RFC3339) + "_" + uid
	return runID
}
