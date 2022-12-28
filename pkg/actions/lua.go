package actions

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/Shopify/go-lua"
	lualibs "github.com/treeverse/lakefs/pkg/actions/lua"
	"github.com/treeverse/lakefs/pkg/actions/lua/lakefs"
	luautil "github.com/treeverse/lakefs/pkg/actions/lua/util"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
)

type LuaHook struct {
	HookBase
	Script     string
	ScriptPath string
	Args       map[string]interface{}
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

func (h *LuaHook) Run(ctx context.Context, record graveler.HookRecord, buf *bytes.Buffer) error {
	user, err := auth.GetUser(ctx)
	if err != nil {
		return err
	}
	l := lua.NewState()
	lualibs.OpenSafe(l, ctx, &loggingBuffer{buf: buf, ctx: ctx})
	injectHookContext(l, ctx, user, h.Endpoint, h.Args)
	applyRecord(l, h.ActionName, h.ID, record)

	// determine if this is an object to load
	code := h.Script
	if h.ScriptPath != "" {
		// load file
		if h.Endpoint == nil {
			return fmt.Errorf("no endpoint configured, cannot request object: %s: %w", h.ScriptPath, ErrInvalidAction)
		}
		url := fmt.Sprintf("/api/v1/repositories/%s/refs/%s/objects", record.RepositoryID, record.SourceRef)
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		req = req.WithContext(auth.WithUser(req.Context(), user))
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
	return err
}

func LuaRun(l *lua.State, code, name string) error {
	var mode string
	if err := lua.LoadBuffer(l, code, name, mode); err != nil {
		return err
	}
	return l.ProtectedCall(0, lua.MultipleReturns, 0)
}

func DescendArgs(args interface{}) (descended interface{}, err error) {
	switch t := args.(type) {
	case Properties:
		for k, v := range t {
			t[k], err = DescendArgs(v)
			if err != nil {
				return
			}
		}
		return t, nil
	case map[string]interface{}:
		for k, v := range t {
			t[k], err = DescendArgs(v)
			if err != nil {
				return
			}
		}
		return t, nil
	case string:
		secure, secureErr := NewSecureString(t)
		if secureErr != nil {
			return t, secureErr
		}
		return secure.val, nil
	case []string:
		stuff := make([]interface{}, len(t))
		for i, e := range t {
			stuff[i], err = DescendArgs(e)
		}
		return stuff, err
	case []interface{}:
		stuff := make([]interface{}, len(t))
		for i, e := range t {
			stuff[i], err = DescendArgs(e)
		}
		return stuff, err
	default:
		return args, nil
	}
}

func NewLuaHook(h ActionHook, action *Action, e *http.Server) (Hook, error) {
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
	parsedArgs, err := DescendArgs(args)
	if err != nil {
		return &LuaHook{}, fmt.Errorf("error parsing args: %w", err)
	}
	args, ok := parsedArgs.(map[string]interface{})
	if !ok {
		return &LuaHook{}, fmt.Errorf("error parsing args, got wrong type: %T: %w", parsedArgs, ErrInvalidAction)
	}

	// script or script_ath
	script, err := h.Properties.getRequiredProperty("script")
	if err == nil {
		return &LuaHook{
			HookBase: HookBase{
				ID:         h.ID,
				ActionName: action.Name,
				Endpoint:   e,
			},
			Script: script,
			Args:   args,
		}, nil
	} else if !errors.Is(err, errMissingKey) {
		// 'script' was provided but is empty or of the wrong type..
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
			Endpoint:   e,
		},
		ScriptPath: scriptFile,
		Args:       args,
	}, nil
}
