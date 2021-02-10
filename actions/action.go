package actions

import (
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"

	"gopkg.in/yaml.v3"
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

type MatchSpec struct {
	EventType EventType
	Branch    string
}

var (
	reHookID = regexp.MustCompile(`^[_a-zA-Z][\-_a-zA-Z0-9]{1,255}$`)

	ErrInvalidAction = errors.New("invalid action")
)

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
		if _, found := hooks[HookType(hook.Type)]; !found {
			return fmt.Errorf("hook[%d] type '%s' unknown: %w", i, hook.ID, ErrInvalidAction)
		}
	}
	return nil
}

func (a *Action) Match(spec MatchSpec) (bool, error) {
	// at least one matched event definition
	var actionOn *ActionOn
	switch spec.EventType {
	case EventTypePreCommit:
		actionOn = a.On.PreCommit
	case EventTypePreMerge:
		actionOn = a.On.PreMerge
	default:
		return false, nil
	}
	// action without branches spec is matched
	if len(actionOn.Branches) == 0 {
		return true, nil
	}
	// find at least one match
	for _, b := range actionOn.Branches {
		matched, err := path.Match(b, spec.Branch)
		if err != nil {
			return false, err
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

// ReadAction helper function to read, parse and validate Action from a reader
func ReadAction(reader io.Reader) (*Action, error) {
	var act Action
	err := yaml.NewDecoder(reader).Decode(&act)
	if err != nil {
		return nil, err
	}
	err = act.Validate()
	if err != nil {
		return nil, err
	}
	return &act, nil
}

type Source interface {
	List() []string
	Load(name string) ([]byte, error)
}

func LoadActions(source Source) ([]*Action, error) {
	return nil, nil
}

func MatchActions(actions []*Action, spec MatchSpec) ([]*Action, error) {
	var matched []*Action
	for _, act := range actions {
		m, err := act.Match(spec)
		if err != nil {
			return nil, err
		}
		if m {
			matched = append(matched, act)
		}
	}
	return matched, nil
}
