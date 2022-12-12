package strings

import (
	"strings"

	"github.com/Shopify/go-lua"
	"github.com/treeverse/lakefs/pkg/actions/lua/util"
)

func Open(l *lua.State) {
	strOpen := func(l *lua.State) int {
		lua.NewLibrary(l, stringLibrary)
		return 1
	}
	lua.Require(l, "strings", strOpen, false)
	l.Pop(1)
}

var stringLibrary = []lua.RegistryFunction{
	{Name: "split", Function: split},
	{Name: "trim", Function: trim},
	{Name: "replace", Function: replace},
	{Name: "has_prefix", Function: hasPrefix},
	{Name: "has_suffix", Function: hasSuffix},
	{Name: "contains", Function: contains},
}

func split(l *lua.State) int {
	str := lua.CheckString(l, 1)
	sep := lua.CheckString(l, 2)

	strArr := strings.Split(str, sep)

	return util.DeepPush(l, strArr)
}

func trim(l *lua.State) int {
	str := lua.CheckString(l, 1)
	l.PushString(strings.TrimSpace(str))
	return 1
}

func replace(l *lua.State) int {
	s := lua.CheckString(l, 1)
	old := lua.CheckString(l, 2)
	new := lua.CheckString(l, 3)
	n := lua.CheckInteger(l, 4)

	l.PushString(strings.Replace(s, old, new, n))
	return 1
}

func hasPrefix(l *lua.State) int {
	s := lua.CheckString(l, 1)
	prefix := lua.CheckString(l, 2)
	l.PushBoolean(strings.HasPrefix(s, prefix))
	return 1
}

func hasSuffix(l *lua.State) int {
	s := lua.CheckString(l, 1)
	suffix := lua.CheckString(l, 2)
	l.PushBoolean(strings.HasSuffix(s, suffix))
	return 1
}

func contains(l *lua.State) int {
	s := lua.CheckString(l, 1)
	substr := lua.CheckString(l, 2)
	l.PushBoolean(strings.Contains(s, substr))
	return 1
}
