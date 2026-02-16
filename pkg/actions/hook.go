package actions

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"

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

type ValidatePropertiesFunc func(Properties) error

// requireProperties returns a ValidatePropertiesFunc that checks required property groups.
// Each group is a slice of keys where at least one must be present (OR within a group).
// All groups must be satisfied (AND between groups).
func requireProperties(hookType string, groups ...[]string) ValidatePropertiesFunc {
	return func(p Properties) error {
		if p == nil {
			return fmt.Errorf("missing properties for %s hook: %w", hookType, ErrInvalidAction)
		}
		for _, group := range groups {
			if !slices.ContainsFunc(group, func(key string) bool {
				_, has := p[key]
				return has
			}) {
				return fmt.Errorf("'%s' must be supplied in properties: %w", strings.Join(group, "' or '"), ErrInvalidAction)
			}
		}
		return nil
	}
}

var hooks = map[HookType]NewHookFunc{
	HookTypeWebhook: NewWebhook,
	HookTypeAirflow: NewAirflowHook,
	HookTypeLua:     NewLuaHook,
}

var hookValidators = map[HookType]ValidatePropertiesFunc{
	HookTypeWebhook: requireProperties("webhook", []string{"url"}),
	HookTypeAirflow: requireProperties("airflow", []string{"url"}, []string{"dag_id"}, []string{"username"}, []string{"password"}),
	HookTypeLua:     requireProperties("lua", []string{"script", "script_path"}),
}

func NewHook(hook ActionHook, action *Action, cfg Config, server *http.Server, serverAddress string, collector stats.Collector) (Hook, error) {
	f := hooks[hook.Type]
	if f == nil {
		return nil, fmt.Errorf("%w (%s)", ErrUnknownHookType, hook.Type)
	}
	return f(hook, action, cfg, server, serverAddress, collector)
}
