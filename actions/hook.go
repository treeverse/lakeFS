package actions

import (
	"context"
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
	Run(ctx context.Context, event Event, writer OutputWriter) error
}

type NewHookFunc func(*Action, ActionHook) Hook

var hooks = map[HookType]NewHookFunc{
	HookTypeWebhook: NewWebhook,
}

func NewHook(h HookType, a *Action, ah ActionHook) Hook {
	f := hooks[h]
	if f == nil {
		return nil
	}
	return f(a, ah)
}
