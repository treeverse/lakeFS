package catalog

import (
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Action struct {
	Name        string       `yaml:"name"`
	Description string       `yaml:"description"`
	On          OnEvents     `yaml:"on"`
	Hooks       []ActionHook `yaml:"hooks"`
}
type OnEvents struct {
	PreMerge  *ActionOn `yaml:"pre-merge"`
	PreCommit *ActionOn `yaml:"pre-commit"`
}

type ActionOn struct {
	Branches []string `yaml:"branches"`
}

type ActionHook struct {
	ID          string            `yaml:"id"`
	Type        string            `yaml:"type"`
	Description string            `yaml:"description"`
	Properties  map[string]string `yaml:"properties"`
}

type ActionCommitData struct {
	Message   string            `json:"message"`
	Committer string            `json:"committer"`
	Metadata  map[string]string `json:"metadata"`
}

type ActionEventData struct {
	RunID        string           `json:"run_id"`
	EventType    string           `json:"event_type"`
	EventTime    string           `json:"event_time"`
	ActionName   string           `json:"action_name"`
	HookID       string           `json:"hook_id"`
	RepositoryID string           `json:"repository_id"`
	BranchID     string           `json:"branch_id"`
	SourceRef    string           `json:"source_ref"`
	Commit       ActionCommitData `json:"commit"`
}

const (
	HookTypeWebhook = "webhook"

	WebhookPropertyKeyURL = "url"
)

var reHookID = regexp.MustCompile(`^[_a-zA-Z][_a-zA-Z0-9]{1,255}$`)

func (a *Action) Validate() error {
	if a.Name == "" {
		return fmt.Errorf("%w 'name' is required", ErrInvalidAction)
	}
	if a.On.PreMerge == nil && a.On.PreCommit == nil {
		return fmt.Errorf("%w 'on' is required", ErrInvalidAction)
	}
	ids := make(map[string]struct{})
	for i, hook := range a.Hooks {
		if !reHookID.MatchString(hook.ID) {
			return fmt.Errorf("hook[%d] missing ID: %w", i, ErrInvalidAction)
		}
		if _, found := ids[hook.ID]; found {
			return fmt.Errorf("hook[%d] duplicate ID '%s': %w", i, hook.ID, ErrInvalidAction)
		}
		ids[hook.ID] = struct{}{}
		if hook.Type != HookTypeWebhook {
			return fmt.Errorf("hook[%d] '%s' unknown type: %w", i, hook.ID, ErrInvalidAction)
		}
	}
	return nil
}

func (a *Action) Match(event string, branch string) (bool, error) {
	var actionOn *ActionOn
	switch event {
	case ActionEventPreCommit:
		actionOn = a.On.PreCommit
	case ActionEventPreMerge:
		actionOn = a.On.PreMerge
	}
	if actionOn == nil {
		return false, nil
	}
	if len(actionOn.Branches) == 0 {
		return true, nil
	}
	for _, b := range actionOn.Branches {
		matched, err := path.Match(b, branch)
		if err != nil {
			return false, err
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

func NewActionEventData(eventType string) ActionEventData {
	now := time.Now().UTC()
	eventTime := now.Format(time.RFC3339)
	uid := strings.ReplaceAll(uuid.New().String(), "-", "")
	runID := eventTime + "_" + uid
	return ActionEventData{
		RunID:     runID,
		EventType: eventType,
		EventTime: eventTime,
	}
}
