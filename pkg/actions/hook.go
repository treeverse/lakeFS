package actions

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type HookType string

const (
	HookTypeWebhook HookType = "webhook"
	HookTypeAirflow HookType = "airflow"
	HookTypeLua     HookType = "lua"
)

// Hook is the abstraction of the basic user-configured runnable building-stone
type Hook interface {
	Run(ctx context.Context, record graveler.HookRecord, buf *bytes.Buffer) error
}

type NewHookFunc func(ActionHook, *Action, *http.Server) (Hook, error)

type HookBase struct {
	ID         string
	ActionName string
	Endpoint   *http.Server
}

var hooks = map[HookType]NewHookFunc{
	HookTypeWebhook: NewWebhook,
	HookTypeAirflow: NewAirflowHook,
	HookTypeLua:     NewLuaHook,
}

var ErrUnknownHookType = errors.New("unknown hook type")

func NewHook(h ActionHook, a *Action, e *http.Server) (Hook, error) {
	f := hooks[h.Type]
	if f == nil {
		return nil, fmt.Errorf("%w (%s)", ErrUnknownHookType, h.Type)
	}
	return f(h, a, e)
}
