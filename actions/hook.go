package actions

//go:generate mockgen -source=hook.go -destination=mock/hook.go -package=mock

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

// OutputWriter is used to write the hook output results to any underline layer
type OutputWriter interface {
	OutputWrite(ctx context.Context, name string, reader io.Reader) error
}

// Hook is the abstraction of the basic user-configured runnable building-stone
type Hook interface {
	Run(ctx context.Context, runID string, event Event, writer OutputWriter) error
}

// Source is an abstraction for reading actions from the storage
type Source interface {
	List() ([]string, error)
	Load(name string) ([]byte, error)
}

type NewHookFunc func(*Action, ActionHook) (Hook, error)

var hooks = map[HookType]NewHookFunc{
	HookTypeWebhook: NewWebhook,
}

var ErrUnknownHookType = errors.New("unknown hook type")

func NewHook(a *Action, ah ActionHook) (Hook, error) {
	f := hooks[HookType(ah.Type)]
	if f == nil {
		return nil, fmt.Errorf("%w (%s)", ErrUnknownHookType, ah)
	}
	return f(a, ah)
}
