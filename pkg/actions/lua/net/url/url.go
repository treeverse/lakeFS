package url

import (
	neturl "net/url"

	"github.com/Shopify/go-lua"
	"github.com/treeverse/lakefs/pkg/actions/lua/util"
)

func Open(l *lua.State) {
	open := func(l *lua.State) int {
		lua.NewLibrary(l, library)
		return 1
	}
	lua.Require(l, "net/url", open, false)
	l.Pop(1)
}

var library = []lua.RegistryFunction{
	{Name: "parse", Function: parse},
	{Name: "query_unescape", Function: queryUnescape},
	{Name: "path_escape", Function: pathEscape},
	{Name: "build_url", Function: build},
}

func parse(l *lua.State) int {
	rawURL := lua.CheckString(l, 1)
	u, err := neturl.Parse(rawURL)
	if err != nil {
		lua.Errorf(l, "%s", err.Error())
		panic("unreachable")
	}
	return util.DeepPush(l, map[string]string{
		"host":     u.Host,
		"path":     u.Path,
		"scheme":   u.Scheme,
		"query":    u.RawQuery,
		"fragment": u.Fragment,
	})
}

func queryUnescape(l *lua.State) int {
	escapedQuery := lua.CheckString(l, 1)
	qu, err := neturl.QueryUnescape(escapedQuery)
	if err != nil {
		lua.Errorf(l, "%s", err.Error())
		panic("unreachable")
	}
	l.PushString(qu)
	return 1
}

func pathEscape(l *lua.State) int {
	path := lua.CheckString(l, 1)
	ep := neturl.PathEscape(path)
	l.PushString(ep)
	return 1
}

func build(l *lua.State) int {
	scheme := lua.CheckString(l, 1)
	host := lua.CheckString(l, 2)
	u := neturl.URL{
		Scheme: scheme,
		Host:   host,
	}
	if !l.IsNone(3) {
		u.Path = lua.CheckString(l, 3)
	}
	if !l.IsNone(4) {
		u.RawQuery = lua.CheckString(l, 4)
	}
	if !l.IsNone(5) {
		u.Fragment = lua.CheckString(l, 3)
	}
	l.PushString(u.String())
	return 1
}
