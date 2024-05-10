package actions

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"time"

	"github.com/Shopify/go-lua"
	"github.com/spf13/viper"

	lualibs "github.com/treeverse/lakefs/pkg/actions/lua"
	"github.com/treeverse/lakefs/pkg/actions/lua/hook"
	"github.com/treeverse/lakefs/pkg/actions/lua/lakefs"
	luautil "github.com/treeverse/lakefs/pkg/actions/lua/util"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
)

type LuaHook struct {
	HookBase
	Script        string
	ScriptPath    string
	Args          map[string]interface{}
	collector     stats.Collector
	serverAddress string
}

func applyRecord(l *lua.State, actionName, hookID string, record graveler.HookRecord) {
	parents := make([]string, len(record.Commit.Parents))
	for i := 0; i < len(record.Commit.Parents); i++ {
		parents[i] = string(record.Commit.Parents[i])
	}
	metadata := make(map[string]string, len(record.Commit.Metadata))
	for k, v := range record.Commit.Metadata {
		metadata[k] = v
	}
	luautil.DeepPush(l, map[string]interface{}{
		"action_name":       actionName,
		"hook_id":           hookID,
		"run_id":            record.RunID,
		"pre_run_id":        record.PreRunID,
		"event_type":        string(record.EventType),
		"commit_id":         record.CommitID.String(),
		"branch_id":         record.BranchID.String(),
		"source_ref":        record.SourceRef.String(),
		"tag_id":            record.TagID.String(),
		"repository_id":     record.RepositoryID.String(),
		"storage_namespace": record.StorageNamespace.String(),
		"commit": map[string]interface{}{
			"message":       record.Commit.Message,
			"meta_range_id": record.Commit.MetaRangeID.String(),
			"creation_date": record.Commit.CreationDate.Format(time.RFC3339),
			"version":       int(record.Commit.Version),
			"metadata":      metadata,
			"parents":       parents,
		},
	})
	l.SetGlobal("action")
}

func injectHookContext(l *lua.State, ctx context.Context, user *model.User, endpoint *http.Server, args map[string]interface{}) {
	l.PushString(user.Username)
	l.SetGlobal("username")
	luautil.DeepPush(l, args)
	l.SetGlobal("args")
	lakefs.OpenClient(l, ctx, user, endpoint)
}

type loggingBuffer struct {
	buf *bytes.Buffer
	ctx context.Context
}

func (l *loggingBuffer) WriteString(s string) (n int, err error) {
	logging.FromContext(l.ctx).WithField("hook_driver", "lua").WithField("hook_output", s).Trace("lua output captured")
	return l.buf.WriteString(s)
}

// allowedFields are the logging fields that are safe to keep on the context
// passed to Lua execution.  These logging fields will enter the Lua script
// and a bug might allow the script to access them.
var allowedFields = []string{logging.RepositoryFieldKey, logging.UserFieldKey}

// getAllowedFields returns only logging fields that are in allowedFields.
func getAllowedFields(fields logging.Fields) logging.Fields {
	// This implementation is efficient when allowedFields is small.
	ret := make(logging.Fields, len(allowedFields))
	for _, f := range allowedFields {
		if v, ok := fields[f]; ok {
			ret[f] = v
		}
	}
	return ret
}

func (h *LuaHook) Run(ctx context.Context, record graveler.HookRecord, buf *bytes.Buffer) error {
	user, err := auth.GetUser(ctx)
	if err != nil {
		return err
	}
	l := lua.NewState()
	osc := lualibs.OpenSafeConfig{
		NetHTTPEnabled: h.Config.Lua.NetHTTPEnabled,
		LakeFSAddr:     h.serverAddress,
	}
	lualibs.OpenSafe(l, ctx, osc, &loggingBuffer{buf: buf, ctx: ctx})
	injectHookContext(l, ctx, user, h.Endpoint, h.Args)
	applyRecord(l, h.ActionName, h.ID, record)

	// determine if this is an object to load
	code := h.Script
	if h.ScriptPath != "" {
		// load file
		if h.Endpoint == nil {
			return fmt.Errorf("no endpoint configured, cannot request object: %s: %w", h.ScriptPath, ErrInvalidAction)
		}
		reqURL, err := url.JoinPath(apiutil.BaseURL,
			"repositories", string(record.RepositoryID), "refs", string(record.SourceRef), "objects")
		if err != nil {
			return err
		}
		req, err := http.NewRequest(http.MethodGet, reqURL, nil)
		if err != nil {
			return err
		}
		req = req.WithContext(auth.WithUser(req.Context(), user))
		req = req.WithContext(logging.AddFields(req.Context(), getAllowedFields(logging.GetFieldsFromContext(ctx))))
		q := req.URL.Query()
		q.Add("path", h.ScriptPath)
		req.URL.RawQuery = q.Encode()
		rr := httptest.NewRecorder()
		h.Endpoint.Handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			return fmt.Errorf("could not load script_path %s: HTTP %d: %w", h.ScriptPath, rr.Code, ErrInvalidAction)
		}
		code = rr.Body.String()
	}
	err = LuaRun(l, code, "lua")
	if err == nil {
		h.collectMetrics(l)
	}
	return err
}

func LuaRun(l *lua.State, code, name string) error {
	l.Global("debug")
	l.Field(-1, "traceback")
	traceback := l.Top()
	var mode string
	if err := lua.LoadBuffer(l, code, name, mode); err != nil {
		v, ok := l.ToString(l.Top())
		if ok {
			err = fmt.Errorf("%w: %s", err, v)
		}
		return err
	}
	if err := l.ProtectedCall(0, lua.MultipleReturns, traceback); err != nil {
		return hook.Unwrap(err)
	}
	return nil
}

func (h *LuaHook) collectMetrics(l *lua.State) {
	const packagePrefix = "lakefs/"
	l.Field(lua.RegistryIndex, "_LOADED")
	l.PushNil()
	for l.Next(-2) {
		key := lua.CheckString(l, -2)
		if strings.HasPrefix(key, packagePrefix) {
			h.collector.CollectEvent(stats.Event{Class: "lua_hooks", Name: key})
		}
		l.Pop(1)
	}
	l.Pop(1) // Pop the _LOADED table from the stack
}

func DescendArgs(args interface{}, getter EnvGetter) (interface{}, error) {
	var err error
	switch t := args.(type) {
	case Properties:
		for k, v := range t {
			t[k], err = DescendArgs(v, getter)
			if err != nil {
				return nil, err
			}
		}
		return t, nil
	case map[string]interface{}:
		for k, v := range t {
			t[k], err = DescendArgs(v, getter)
			if err != nil {
				return nil, err
			}
		}
		return t, nil
	case string:
		secure, secureErr := NewSecureString(t, getter)
		if secureErr != nil {
			return t, secureErr
		}
		return secure.val, nil
	case []string:
		stuff := make([]interface{}, len(t))
		for i, e := range t {
			stuff[i], err = DescendArgs(e, getter)
		}
		return stuff, err
	case []interface{}:
		stuff := make([]interface{}, len(t))
		for i, e := range t {
			stuff[i], err = DescendArgs(e, getter)
		}
		return stuff, err
	default:
		return args, nil
	}
}

func NewLuaHook(h ActionHook, action *Action, cfg Config, e *http.Server, serverAddress string, collector stats.Collector) (Hook, error) {
	// optional args
	args := make(map[string]interface{})
	argsVal, hasArgs := h.Properties["args"]
	if hasArgs {
		switch typedArgs := argsVal.(type) {
		case Properties:
			for k, v := range typedArgs {
				args[k] = v
			}
		case map[string]interface{}:
			args = typedArgs
		default:
			return nil, fmt.Errorf("'args' should be a map: %w", errWrongValueType)
		}
	}
	parsedArgs, err := DescendArgs(args, &EnvironmentVariableGetter{
		Enabled: cfg.Env.Enabled,
		Prefix:  viper.GetEnvPrefix(),
	})
	if err != nil {
		return &LuaHook{}, fmt.Errorf("error parsing args: %w", err)
	}
	args, ok := parsedArgs.(map[string]interface{})
	if !ok {
		return &LuaHook{}, fmt.Errorf("error parsing args, got wrong type: %T: %w", parsedArgs, ErrInvalidAction)
	}

	// script or script_path
	script, err := h.Properties.getRequiredProperty("script")
	if err == nil {
		return &LuaHook{
			HookBase: HookBase{
				ID:         h.ID,
				ActionName: action.Name,
				Config:     cfg,
				Endpoint:   e,
			},
			Script:        script,
			Args:          args,
			collector:     collector,
			serverAddress: serverAddress,
		}, nil
	} else if !errors.Is(err, errMissingKey) {
		// 'script' was provided but is empty or of the wrong type.
		return nil, err
	}

	// no script provided, let's see if we have a script path
	scriptFile, err := h.Properties.getRequiredProperty("script_path")
	if err != nil && errors.Is(err, errMissingKey) {
		return nil, fmt.Errorf("'script' or 'script_path' must be supplied: %w", errWrongValueType)
	} else if err != nil {
		return nil, err
	}

	return &LuaHook{
		HookBase: HookBase{
			ID:         h.ID,
			ActionName: action.Name,
			Config:     cfg,
			Endpoint:   e,
		},
		ScriptPath:    scriptFile,
		Args:          args,
		collector:     collector,
		serverAddress: serverAddress,
	}, nil
}
