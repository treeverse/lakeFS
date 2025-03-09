package lakefs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"

	"github.com/Shopify/go-lua"
	"github.com/go-chi/chi/v5"
	"github.com/treeverse/lakefs/pkg/actions/lua/util"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/version"
)

// LuaClientUserAgent is the default user agent that will be sent to the lakeFS server instance
var LuaClientUserAgent = "lakefs-lua/" + version.Version

func check(l *lua.State, err error) {
	if err != nil {
		lua.Errorf(l, "%s", err.Error())
		panic("unreachable")
	}
}

func newLakeFSRequest(ctx context.Context, user *model.User, method, reqURL string, data []byte) (*http.Request, error) {
	if !strings.HasPrefix(reqURL, "/api/") {
		var err error
		reqURL, err = url.JoinPath(apiutil.BaseURL, reqURL)
		if err != nil {
			return nil, err
		}
	}

	var body io.Reader
	if data != nil {
		body = bytes.NewReader(data)
	}

	// Set no_hooks to true to disable recursive hooks
	ctx = context.WithValue(ctx, graveler.ContextKeyNoHooks, true)

	// Chi stores its routing information on the request context which breaks this sub-request's routing.
	// We explicitly nullify any existing routing information before creating the new request
	ctx = context.WithValue(ctx, chi.RouteCtxKey, nil)
	// Add user to the request context
	ctx = auth.WithUser(ctx, user)
	req, err := http.NewRequestWithContext(ctx, method, reqURL, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", LuaClientUserAgent)
	return req, nil
}

func newLakeFSJSONRequest(ctx context.Context, user *model.User, method, reqURL string, data []byte) (*http.Request, error) {
	req, err := newLakeFSRequest(ctx, user, method, reqURL, data)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

func getLakeFSJSONResponse(l *lua.State, server *http.Server, request *http.Request) int {
	rr := httptest.NewRecorder()
	server.Handler.ServeHTTP(rr, request)
	l.PushInteger(rr.Code)
	if rr.Body.Len() == 0 {
		return 1
	}

	var output any
	check(l, json.Unmarshal(rr.Body.Bytes(), &output))
	return 1 + util.DeepPush(l, output)
}

// updateObjectUserMetadata handles updating object metadata
func updateObjectUserMetadata(l *lua.State, ctx context.Context, user *model.User, server *http.Server) int {
	repo := lua.CheckString(l, 1)
	branch := lua.CheckString(l, 2)
	objPath := lua.CheckString(l, 3)
	metadata, err := util.PullStringTable(l, 4)
	check(l, err)

	data, err := json.Marshal(map[string]any{
		"set": metadata,
	})
	check(l, err)

	reqURL, err := url.JoinPath("/repositories", repo, "branches", branch, "objects/stat/user_metadata")
	check(l, err)

	req, err := newLakeFSJSONRequest(ctx, user, http.MethodPut, reqURL, data)
	check(l, err)

	// query params
	q := req.URL.Query()
	q.Add("path", objPath)
	req.URL.RawQuery = q.Encode()

	return getLakeFSJSONResponse(l, server, req)
}

// createTag handles tag creation
func createTag(l *lua.State, ctx context.Context, user *model.User, server *http.Server) int {
	repo := lua.CheckString(l, 1)
	data, err := json.Marshal(map[string]string{
		"ref": lua.CheckString(l, 2),
		"id":  lua.CheckString(l, 3),
	})
	check(l, err)
	reqURL, err := url.JoinPath("/repositories", repo, "tags")
	check(l, err)
	req, err := newLakeFSJSONRequest(ctx, user, http.MethodPost, reqURL, data)
	check(l, err)
	return getLakeFSJSONResponse(l, server, req)
}

// diffRefs handles comparing two refs
func diffRefs(l *lua.State, ctx context.Context, user *model.User, server *http.Server) int {
	repo := lua.CheckString(l, 1)
	leftRef := lua.CheckString(l, 2)
	rightRef := lua.CheckString(l, 3)
	reqURL, err := url.JoinPath("/repositories", repo, "refs", leftRef, "diff", rightRef)
	check(l, err)
	req, err := newLakeFSJSONRequest(ctx, user, http.MethodGet, reqURL, nil)
	check(l, err)
	// query params
	q := req.URL.Query()
	if !l.IsNone(4) {
		q.Add("after", lua.CheckString(l, 4))
	}
	if !l.IsNone(5) {
		q.Add("prefix", lua.CheckString(l, 5))
	}
	if !l.IsNone(6) {
		q.Add("delimiter", lua.CheckString(l, 6))
	}
	if !l.IsNone(7) {
		q.Add("amount", fmt.Sprintf("%d", lua.CheckInteger(l, 7)))
	}
	req.URL.RawQuery = q.Encode()
	return getLakeFSJSONResponse(l, server, req)
}

// listObjects handles listing objects in a ref
func listObjects(l *lua.State, ctx context.Context, user *model.User, server *http.Server) int {
	repo := lua.CheckString(l, 1)
	ref := lua.CheckString(l, 2)
	reqURL, err := url.JoinPath("/repositories", repo, "refs", ref, "objects/ls")
	check(l, err)
	req, err := newLakeFSJSONRequest(ctx, user, http.MethodGet, reqURL, nil)
	check(l, err)
	// query params
	q := req.URL.Query()
	if !l.IsNone(3) {
		q.Add("after", lua.CheckString(l, 3))
	}
	if !l.IsNone(4) {
		q.Add("prefix", lua.CheckString(l, 4))
	}
	if !l.IsNone(5) {
		q.Add("delimiter", lua.CheckString(l, 5))
	}
	if !l.IsNone(6) {
		q.Add("amount", fmt.Sprintf("%d", lua.CheckInteger(l, 6)))
	}
	if !l.IsNone(7) {
		withUserMetadata := "false"
		if l.ToBoolean(7) {
			withUserMetadata = "true"
		}
		q.Add("user_metadata", withUserMetadata)
	}
	req.URL.RawQuery = q.Encode()
	return getLakeFSJSONResponse(l, server, req)
}

// getObject handles retrieving an object
func getObject(l *lua.State, ctx context.Context, user *model.User, server *http.Server) int {
	repo := lua.CheckString(l, 1)
	ref := lua.CheckString(l, 2)
	reqURL, err := url.JoinPath("/repositories", repo, "refs", ref, "objects")
	check(l, err)
	req, err := newLakeFSJSONRequest(ctx, user, http.MethodGet, reqURL, nil)
	check(l, err)
	// query params
	q := req.URL.Query()
	q.Add("path", lua.CheckString(l, 3))
	req.URL.RawQuery = q.Encode()
	rr := httptest.NewRecorder()
	server.Handler.ServeHTTP(rr, req)
	l.PushInteger(rr.Code)
	l.PushString(rr.Body.String())
	return 2
}

// statObject handles retrieving object stats
func statObject(l *lua.State, ctx context.Context, user *model.User, server *http.Server) int {
	repo := lua.CheckString(l, 1)
	ref := lua.CheckString(l, 2)
	reqURL, err := url.JoinPath("/repositories", repo, "refs", ref, "objects", "stat")
	check(l, err)
	req, err := newLakeFSJSONRequest(ctx, user, http.MethodGet, reqURL, nil)
	check(l, err)
	// query params
	q := req.URL.Query()
	q.Add("path", lua.CheckString(l, 3))
	if !l.IsNone(4) {
		userMetadata := strconv.FormatBool(l.ToBoolean(4))
		q.Add("user_metadata", userMetadata)
	}
	req.URL.RawQuery = q.Encode()
	rr := httptest.NewRecorder()
	server.Handler.ServeHTTP(rr, req)
	l.PushInteger(rr.Code)
	l.PushString(rr.Body.String())
	return 2
}

// diffBranch handles comparing a branch
func diffBranch(l *lua.State, ctx context.Context, user *model.User, server *http.Server) int {
	repo := lua.CheckString(l, 1)
	branch := lua.CheckString(l, 2)
	reqURL, err := url.JoinPath("/repositories", repo, "branches", branch, "diff")
	check(l, err)
	req, err := newLakeFSJSONRequest(ctx, user, http.MethodGet, reqURL, nil)
	check(l, err)
	// query params
	q := req.URL.Query()
	if !l.IsNone(3) {
		q.Add("after", lua.CheckString(l, 3))
	}
	if !l.IsNone(4) {
		q.Add("amount", fmt.Sprintf("%d", lua.CheckInteger(l, 4)))
	}
	if !l.IsNone(5) {
		q.Add("prefix", lua.CheckString(l, 5))
	}
	if !l.IsNone(6) {
		q.Add("delimiter", lua.CheckString(l, 6))
	}
	req.URL.RawQuery = q.Encode()
	return getLakeFSJSONResponse(l, server, req)
}

// commitBranch handles committing changes to a branch
func commitBranch(l *lua.State, ctx context.Context, user *model.User, server *http.Server) int {
	repo := lua.CheckString(l, 1)
	branch := lua.CheckString(l, 2)
	message := lua.CheckString(l, 3)

	data := map[string]any{
		"message": message,
	}

	// Handle optional parameters table if provided (4th parameter)
	if !l.IsNone(4) {
		table, err := util.PullTable(l, 4)
		check(l, err)
		tableMap := table.(map[string]interface{})
		// check if allow_empty is a boolean, error if not
		if allowEmpty, ok := tableMap["allow_empty"]; ok {
			if allowEmptyBool, ok := allowEmpty.(bool); ok {
				data["allow_empty"] = allowEmptyBool
			} else {
				lua.Errorf(l, "allow_empty must be a boolean")
				panic("unreachable")
			}
		}

		// check if metadata is a table, error if not
		if metadata, ok := tableMap["metadata"]; ok {
			if metadataTable, ok := metadata.(map[string]any); ok {
				data["metadata"] = metadataTable
			} else {
				lua.Errorf(l, "metadata must be a table")
				panic("unreachable")
			}
		}
	}

	jsonData, err := json.Marshal(data)
	check(l, err)

	reqURL, err := url.JoinPath("/repositories", repo, "branches", branch, "commits")
	check(l, err)

	req, err := newLakeFSJSONRequest(ctx, user, http.MethodPost, reqURL, jsonData)
	check(l, err)

	return getLakeFSJSONResponse(l, server, req)
}

// OpenClient opens a new lakeFS client with the given context, user and server
func OpenClient(l *lua.State, ctx context.Context, user *model.User, server *http.Server) {
	clientOpen := func(l *lua.State) int {
		lua.NewLibrary(l, []lua.RegistryFunction{
			{Name: "update_object_user_metadata", Function: func(state *lua.State) int {
				return updateObjectUserMetadata(l, ctx, user, server)
			}},
			{Name: "create_tag", Function: func(state *lua.State) int {
				return createTag(l, ctx, user, server)
			}},
			{Name: "diff_refs", Function: func(state *lua.State) int {
				return diffRefs(l, ctx, user, server)
			}},
			{Name: "list_objects", Function: func(state *lua.State) int {
				return listObjects(l, ctx, user, server)
			}},
			{Name: "get_object", Function: func(state *lua.State) int {
				return getObject(l, ctx, user, server)
			}},
			{Name: "stat_object", Function: func(state *lua.State) int {
				return statObject(l, ctx, user, server)
			}},
			{Name: "diff_branch", Function: func(state *lua.State) int {
				return diffBranch(l, ctx, user, server)
			}},
			{Name: "commit", Function: func(state *lua.State) int {
				return commitBranch(l, ctx, user, server)
			}},
		})
		return 1
	}
	lua.Require(l, "lakefs", clientOpen, false)
	l.Pop(1)
}
