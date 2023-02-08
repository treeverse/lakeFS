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
	"strings"

	"github.com/Shopify/go-lua"
	"github.com/go-chi/chi/v5"
	"github.com/treeverse/lakefs/pkg/actions/lua/util"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/version"
)

// LuaClientUserAgent is the default user agent that will be sent to the lakeFS server instance
var LuaClientUserAgent = "lakefs-lua/" + version.Version

func check(l *lua.State, err error) {
	if err != nil {
		lua.Errorf(l, err.Error())
		panic("unreachable")
	}
}

func newLakeFSRequest(ctx context.Context, user *model.User, method, url string, data []byte) (*http.Request, error) {
	if !strings.HasPrefix(url, "/api/") {
		if strings.HasPrefix(url, "/") {
			url = fmt.Sprintf("/api/v1%s", url)
		} else {
			url = fmt.Sprintf("/api/v1/%s", url)
		}
	}
	var body io.Reader
	if data == nil {
		body = bytes.NewReader(data)
	}

	// Chi stores its routing information on the request context which breaks this sub-request's routing.
	// We explicitly nullify any existing routing information before creating the new request
	ctx = context.WithValue(ctx, chi.RouteCtxKey, nil)
	// Add user to the request context
	ctx = auth.WithUser(ctx, user)
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", LuaClientUserAgent)
	return req, nil
}

func newLakeFSJSONRequest(ctx context.Context, user *model.User, method, url string, data []byte) (*http.Request, error) {
	req, err := newLakeFSRequest(ctx, user, method, url, data)
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

	var output interface{}
	check(l, json.Unmarshal(rr.Body.Bytes(), &output))
	return 1 + util.DeepPush(l, output)
}

func OpenClient(l *lua.State, ctx context.Context, user *model.User, server *http.Server) {
	clientOpen := func(l *lua.State) int {
		lua.NewLibrary(l, []lua.RegistryFunction{
			{Name: "create_tag", Function: func(state *lua.State) int {
				repo := lua.CheckString(l, 1)
				data, err := json.Marshal(map[string]string{
					"ref": lua.CheckString(l, 2),
					"id":  lua.CheckString(l, 3),
				})
				if err != nil {
					check(l, err)
				}

				path := fmt.Sprintf("/repositories/%s/tags", url.PathEscape(repo))
				req, err := newLakeFSJSONRequest(ctx, user, http.MethodPost, path, data)
				if err != nil {
					check(l, err)
				}
				return getLakeFSJSONResponse(l, server, req)
			}},
			{Name: "diff_refs", Function: func(state *lua.State) int {
				repo := lua.CheckString(l, 1)
				leftRef := lua.CheckString(l, 2)
				rightRef := lua.CheckString(l, 3)
				reqURL := fmt.Sprintf("/repositories/%s/refs/%s/diff/%s", url.PathEscape(repo), url.PathEscape(leftRef), url.PathEscape(rightRef))
				req, err := newLakeFSJSONRequest(ctx, user, http.MethodGet, reqURL, nil)
				if err != nil {
					check(l, err)
				}
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
			}},
			{Name: "list_objects", Function: func(state *lua.State) int {
				repo := lua.CheckString(l, 1)
				ref := lua.CheckString(l, 2)
				reqURL := fmt.Sprintf("/repositories/%s/refs/%s/objects/ls", url.PathEscape(repo), url.PathEscape(ref))
				req, err := newLakeFSJSONRequest(ctx, user, http.MethodGet, reqURL, nil)
				if err != nil {
					check(l, err)
				}
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
			}},
			{Name: "get_object", Function: func(state *lua.State) int {
				repo := lua.CheckString(l, 1)
				ref := lua.CheckString(l, 2)
				reqURL := fmt.Sprintf("/repositories/%s/refs/%s/objects", url.PathEscape(repo), url.PathEscape(ref))
				req, err := newLakeFSJSONRequest(ctx, user, http.MethodGet, reqURL, nil)
				if err != nil {
					check(l, err)
				}
				// query params
				q := req.URL.Query()
				q.Add("path", lua.CheckString(l, 3))
				req.URL.RawQuery = q.Encode()
				rr := httptest.NewRecorder()
				server.Handler.ServeHTTP(rr, req)
				l.PushInteger(rr.Code)
				l.PushString(rr.Body.String())
				return 2
			}},
		})
		return 1
	}
	lua.Require(l, "lakefs", clientOpen, false)
	l.Pop(1)
}
