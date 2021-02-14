package actions

import (
	"errors"
	"fmt"
	"path"
	"regexp"

	"github.com/hashicorp/go-multierror"
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

	ErrInvalidAction    = errors.New("invalid action")
	ErrInvalidEventType = errors.New("invalid event type")
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
		return false, ErrInvalidEventType
	}
	// if no action specified - no match
	if actionOn == nil {
		return false, nil
	}
	// if no branches spec found - all match
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

// ParseAction helper function to read, parse and validate Action from a reader
func ParseAction(data []byte) (*Action, error) {
	var act Action
	err := yaml.Unmarshal(data, &act)
	if err != nil {
		return nil, err
	}
	err = act.Validate()
	if err != nil {
		return nil, err
	}
	return &act, nil
}

func LoadActions(source Source) ([]*Action, error) {
	hooksAddresses, err := source.List()
	if err != nil {
		return nil, fmt.Errorf("list actions from commit: %w", err)
	}

	actions := make([]*Action, len(hooksAddresses))
	var errGroup multierror.Group
	for i := range hooksAddresses {
		// pin i for embedded func
		ii := i
		errGroup.Go(func() error {
			addr := hooksAddresses[ii]
			bytes, err := source.Load(addr)
			if err != nil {
				return fmt.Errorf("loading file %s: %w", addr, err)
			}
			action, err := ParseAction(bytes)
			if err != nil {
				return fmt.Errorf("parsing file %s: %w", addr, err)
			}
			actions[ii] = action

			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	return actions, nil
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
