package actions

import (
	"context"
	"errors"
	"fmt"
	"path"
	"regexp"

	"github.com/hashicorp/go-multierror"
	"github.com/treeverse/lakefs/pkg/graveler"
	"gopkg.in/yaml.v3"
)

type Action struct {
	Name        string                           `yaml:"name"`
	Description string                           `yaml:"description"`
	On          map[graveler.EventType]*ActionOn `yaml:"on"`
	Hooks       []ActionHook                     `yaml:"hooks"`
}

type ActionOn struct {
	Branches []string `yaml:"branches"`
}

var (
	errMissingKey     = errors.New("missing key in properties")
	errMissingEnvVar  = errors.New("missing env var")
	errWrongValueType = errors.New("wrong value type")
)

type Properties map[string]interface{}

func (p Properties) getRequiredProperty(key string) (string, error) {
	raw, ok := p[key]
	if !ok {
		return "", fmt.Errorf("key %s: %w", key, errMissingKey)
	}

	val, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("value of %s is not of type string: %w", key, errWrongValueType)
	}

	if val == "" {
		return "", fmt.Errorf("value of %s is empty: %w", key, errMissingKey)
	}

	return val, nil
}

type ActionHook struct {
	ID          string     `yaml:"id"`
	Type        HookType   `yaml:"type"`
	Description string     `yaml:"description"`
	Properties  Properties `yaml:"properties"`
}

type MatchSpec struct {
	EventType graveler.EventType
	BranchID  graveler.BranchID
}

var (
	reName   = regexp.MustCompile(`^\w[\w\-. ]+$`)
	reHookID = regexp.MustCompile(`^[_a-zA-Z][\-_a-zA-Z0-9]{1,255}$`)

	ErrInvalidAction    = errors.New("invalid action")
	ErrInvalidEventType = errors.New("invalid event type")
)

var supportedEvents = map[graveler.EventType]bool{
	graveler.EventTypePreCommit:        true,
	graveler.EventTypePostMerge:        true,
	graveler.EventTypePreMerge:         true,
	graveler.EventTypePostCommit:       true,
	graveler.EventTypePreCreateBranch:  true,
	graveler.EventTypePostCreateBranch: true,
	graveler.EventTypePreDeleteBranch:  true,
	graveler.EventTypePostDeleteBranch: true,
	graveler.EventTypePreCreateTag:     true,
	graveler.EventTypePostCreateTag:    true,
	graveler.EventTypePreDeleteTag:     true,
	graveler.EventTypePostDeleteTag:    true,
}

func (a *Action) Validate() error {
	if a.Name == "" {
		return fmt.Errorf("'name' is required: %w", ErrInvalidAction)
	}
	if !reName.MatchString(a.Name) {
		return fmt.Errorf("'name' is invalid: %w", ErrInvalidAction)
	}
	if a.On == nil || len(a.On) == 0 {
		return fmt.Errorf("'on' is required: %w", ErrInvalidAction)
	}
	// TODO: Add check by hook type for content (branches etc.)
	for o := range a.On {
		if !supportedEvents[o] {
			return fmt.Errorf("event '%s' is not supported: %w", o, ErrInvalidAction)
		}
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
		if _, found := hooks[hook.Type]; !found {
			return fmt.Errorf("hook[%d] type '%s' unknown: %w", i, hook.ID, ErrInvalidAction)
		}
	}
	return nil
}

func (a *Action) Match(spec MatchSpec) (bool, error) {
	// at least one matched event definition
	actionOn, ok := a.On[spec.EventType]
	// if no action specified - no match
	if !ok {
		return false, nil
	}
	// if no branches spec found - all match
	if actionOn == nil || len(actionOn.Branches) == 0 {
		return true, nil
	}
	// find at least one match
	branchSpec := spec.BranchID.String()
	for _, b := range actionOn.Branches {
		matched, err := path.Match(b, branchSpec)
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

func LoadActions(ctx context.Context, source Source, record graveler.HookRecord) ([]*Action, error) {
	hooksAddresses, err := source.List(ctx, record)
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
			bytes, err := source.Load(ctx, record, addr)
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
	if err := validateActions(actions); err != nil {
		return nil, err
	}
	return actions, nil
}

// validateActions verify we do not two actions with the same name
func validateActions(actions []*Action) error {
	actionNames := make(map[string]struct{})
	for _, action := range actions {
		if _, found := actionNames[action.Name]; found {
			return fmt.Errorf("action name '%s' already loaded: %w", action.Name, ErrInvalidAction)
		}
		actionNames[action.Name] = struct{}{}
	}
	return nil
}

func MatchedActions(actions []*Action, spec MatchSpec) ([]*Action, error) {
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
