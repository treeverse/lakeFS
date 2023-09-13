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
	{Name: "parse", Function: Parse},
}

func Parse(l *lua.State) int {
	rawURL := lua.CheckString(l, 1)
	u, err := neturl.Parse(rawURL)
	if err != nil {
		lua.Errorf(l, err.Error())
		panic("unreachable")
	}
	return util.DeepPush(l, map[string]string{
		"host":   u.Host,
		"path":   u.Path,
		"scheme": u.Scheme,
		"query":  u.RawQuery,
	})
}
