package catalog

import (
	"errors"
	"fmt"
	"regexp"
)

type Action struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
	On          HookOn `yaml:"on"`
	Hooks       []Hook `yaml:"hooks"`
}

type HookOnAction struct {
	Branches []string `yaml:"branches"`
}

type HookOn struct {
	PreMerge  *HookOnAction `yaml:"pre-merge"`
	PreCommit *HookOnAction `yaml:"pre-commit"`
}

type HookProperties struct {
	URL string `yaml:"url"`
}

type Hook struct {
	ID          string         `yaml:"id"`
	Type        string         `yaml:"type"`
	Description string         `yaml:"description"`
	Properties  HookProperties `yaml:"properties"`
}

var (
	ErrInvalidAction = errors.New("invalid action")

	reHookID = regexp.MustCompile(`^[_a-zA-Z][_a-zA-Z0-9]{1,255}$`)
)

func (a *Action) Validate() error {
	if a.On.PreMerge == nil && a.On.PreCommit == nil {
		return fmt.Errorf("%w 'on' is required", ErrInvalidAction)
	}
	ids := make(map[string]struct{})
	for i, hook := range a.Hooks {
		if !reHookID.MatchString(hook.ID) {
			return fmt.Errorf("%w hook[%d] missing ID", ErrInvalidAction, i)
		}
		if _, found := ids[hook.ID]; found {
			return fmt.Errorf("%w hook[%d] duplicate ID", ErrInvalidAction, i)
		}
		ids[hook.ID] = struct{}{}
		if hook.Type != "webhook" {
			return fmt.Errorf("%w hook[%d] unknown type", ErrInvalidAction, i)
		}
	}
	return nil
}
