package actions

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/stats"
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

type NewHookFunc func(ActionHook, *Action, Config, *http.Server, string, stats.Collector) (Hook, error)

type HookBase struct {
	ID         string
	ActionName string
	Config     Config
	Endpoint   *http.Server
}

var hooks = map[HookType]NewHookFunc{
	HookTypeWebhook: NewWebhook,
	HookTypeAirflow: NewAirflowHook,
	HookTypeLua:     NewLuaHook,
}

func NewHook(hook ActionHook, action *Action, cfg Config, server *http.Server, serverAddress string, collector stats.Collector) (Hook, error) {
	f := hooks[hook.Type]
	if f == nil {
		return nil, fmt.Errorf("%w (%s)", ErrUnknownHookType, hook.Type)
	}
	return f(hook, action, cfg, server, serverAddress, collector)
}
