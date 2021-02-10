package actions

import (
	"context"
	"errors"
	"fmt"
	"io"
)

type HookType string

const (
	HookTypeWebhook HookType = "webhook"
)

type OutputWriter interface {
	OutputWrite(ctx context.Context, name string, reader io.Reader) error
}

type Hook interface {
	Run(ctx context.Context, runID string, event Event, writer OutputWriter) error
}

type NewHookFunc func(*Action, ActionHook) (Hook, error)

var hooks = map[HookType]NewHookFunc{
	HookTypeWebhook: NewWebhook,
}

var ErrUnknownHookType = errors.New("unknown hook type")

func NewHook(h HookType, a *Action, ah ActionHook) (Hook, error) {
	f := hooks[h]
	if f == nil {
		return nil, fmt.Errorf("%w (%s)", ErrUnknownHookType, h)
	}
	return f(a, ah)
}
