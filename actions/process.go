package actions

import (
	"context"

	"github.com/hashicorp/go-multierror"
)

type Process struct {
	source Source
	writer OutputWriter
	event  Event
}

type runRecord struct {
	Action *Action
	HookID string
	Hook   Hook
}

func NewProcess(source Source, writer OutputWriter, event Event) *Process {
	return &Process{
		source: source,
		event:  event,
		writer: writer,
	}
}

func (p *Process) Run(ctx context.Context) error {
	matched, err := p.loadMatchedActions()
	if err != nil || len(matched) == 0 {
		return err
	}
	records, err := prepareRunRecords(matched)
	if err != nil || len(records) == 0 {
		return err
	}
	runID := NewRunID(p.event.EventTime)
	var g multierror.Group
	for _, record := range records {
		r := record // pin record
		g.Go(func() error {
			hookWriter := &HookWriter{
				RunID:      runID,
				ActionName: r.Action.Name,
				HookID:     r.HookID,
				Writer:     p.writer,
			}
			return r.Hook.Run(ctx, runID, p.event, hookWriter)
		})
	}
	return g.Wait().ErrorOrNil()
}

func (p *Process) loadMatchedActions() ([]*Action, error) {
	actions, err := LoadActions(p.source)
	if err != nil {
		return nil, err
	}
	matched, err := MatchActions(actions, MatchSpec{
		EventType: p.event.EventType,
		Branch:    p.event.BranchID,
	})
	if err != nil {
		return nil, err
	}
	return matched, nil
}

func prepareRunRecords(actions []*Action) ([]runRecord, error) {
	var actionHooks []runRecord
	for _, action := range actions {
		for _, hook := range action.Hooks {
			h, err := NewHook(hook, action)
			if err != nil {
				return nil, err
			}
			actionHooks = append(actionHooks, runRecord{
				Action: action,
				HookID: hook.ID,
				Hook:   h,
			})
		}
	}
	return actionHooks, nil
}
