package actions

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type HookType string

const (
	HookTypeWebhook HookType = "webhook"
	HookTypeAirflow HookType = "airflow"
)

// Hook is the abstraction of the basic user-configured runnable building-stone
type Hook interface {
	Run(ctx context.Context, record graveler.HookRecord, buf *bytes.Buffer) error
}

type NewHookFunc func(ActionHook, *Action) (Hook, error)

type HookBase struct {
	ID         string
	ActionName string
}

var hooks = map[HookType]NewHookFunc{
	HookTypeWebhook: NewWebhook,
	HookTypeAirflow: NewAirflowHook,
}

var ErrUnknownHookType = errors.New("unknown hook type")

func NewHook(h ActionHook, a *Action) (Hook, error) {
	f := hooks[h.Type]
	if f == nil {
		return nil, fmt.Errorf("%w (%s)", ErrUnknownHookType, h.Type)
	}
	return f(h, a)
}
