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

// Hook is the abstraction of the basic user-configured runnable building-stone
type Hook interface {
	Run(ctx context.Context, event Event, writer *HookOutputWriter) error
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
