package actions

import (
	"context"
	"errors"
	"fmt"
)

type HookType string

const (
	HookTypeWebhook HookType = "webhook"
)

type Hook interface {
	Run(ctx context.Context, event Event) error
}

type NewHookFunc func(ActionHook, *Action) (Hook, error)

var hooks = map[HookType]NewHookFunc{
	HookTypeWebhook: NewWebhook,
}

var ErrUnknownHookType = errors.New("unknown hook type")

func NewHook(h ActionHook, a *Action) (Hook, error) {
	f := hooks[h.Type]
	if f == nil {
		return nil, fmt.Errorf("%w (%s)", ErrUnknownHookType, h.Type)
	}
	return f(h, a)
}
