package http

import (
	"io"
	"net/http"
	"strings"

	"github.com/Shopify/go-lua"
	"github.com/treeverse/lakefs/pkg/actions/lua/util"
)

func Open(l *lua.State) {
	open := func(l *lua.State) int {
		lua.NewLibrary(l, httpLibrary)
		return 1
	}
	lua.Require(l, "net/http", open, false)
	l.Pop(1)
}

var httpLibrary = []lua.RegistryFunction{
	{Name: "request", Function: httpRequest},
}

// httpRequest - perform http request
//
//		Accepts arguments (url, body) or table with url, method, body, headers. Value for url is required. method is by default GET.
//	 Returns code, body, headers, status.
func httpRequest(l *lua.State) int {
	var (
		reqMethod  = http.MethodGet
		reqURL     string
		reqBody    io.Reader
		reqHeaders map[string]interface{}
	)
	switch l.TypeOf(1) {
	case lua.TypeString:
		reqURL = lua.CheckString(l, 1)
		// get optional body as string
		if s, ok := l.ToString(2); ok {
			reqBody = strings.NewReader(s)
		}
	case lua.TypeTable:
		// extract request params from table
		value, err := util.PullTable(l, 1)
		check(l, err)
		tbl := value.(map[string]interface{})
		if s, ok := tbl["url"].(string); ok {
			reqURL = s
		}
		if s, ok := tbl["method"].(string); ok {
			reqMethod = s
		}
		if s, ok := tbl["body"].(string); ok {
			reqBody = strings.NewReader(s)
		}
		if m, ok := tbl["headers"].(map[string]interface{}); ok {
			reqHeaders = m
		}
	default:
		lua.Errorf(l, "first argument can be url or request table (invalid type: %d)", l.TypeOf(1))
		panic("unreachable")
	}
	if reqURL == "" {
		lua.Errorf(l, "missing request url")
		panic("unreachable")
	}
	req, err := http.NewRequest(reqMethod, reqURL, reqBody)
	check(l, err)
	requestAddHeader(reqHeaders, req)
	resp, err := http.DefaultClient.Do(req)
	check(l, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	check(l, err)
	l.PushInteger(resp.StatusCode)
	l.PushString(string(body))
	pushResponseHeader(l, resp.Header)
	l.PushString(resp.Status)
	return 4
}

// requestAddHeader add headers to request. each table value can be single a string or array(table) of strings
func requestAddHeader(reqHeaders map[string]interface{}, req *http.Request) {
	for k, v := range reqHeaders {
		switch vv := v.(type) {
		case string:
			req.Header.Add(k, vv)
		case map[string]interface{}:
			for _, val := range vv {
				if s, ok := val.(string); ok {
					req.Header.Add(k, s)
				}
			}
		}
	}
}

// pushResponseHeader response headers as table. the result table will include single value or table of values for multiple values
func pushResponseHeader(l *lua.State, header http.Header) {
	m := make(map[string]interface{}, len(header))
	for k, v := range header {
		if len(v) == 1 {
			m[k] = v[0]
		} else {
			m[k] = v
		}
	}
	util.DeepPush(l, m)
}

func check(l *lua.State, err error) {
	if err != nil {
		lua.Errorf(l, err.Error())
		panic("unreachable")
	}
}
