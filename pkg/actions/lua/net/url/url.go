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
