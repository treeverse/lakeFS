package path

import (
	"strings"

	"github.com/Shopify/go-lua"
	"github.com/treeverse/lakefs/pkg/actions/lua/util"
)

const (
	SEPARATOR    = "/"
	HiddenPrefix = "_"
)

func Open(l *lua.State) {
	open := func(l *lua.State) int {
		lua.NewLibrary(l, library)
		return 1
	}
	lua.Require(l, "path", open, false)
	l.Pop(1)
}

var library = []lua.RegistryFunction{
	{Name: "parse", Function: parse},
	{Name: "join", Function: join},
	{Name: "is_hidden", Function: isHidden},
	{Name: "default_separator", Function: getDefaultSeparator},
}

func getDefaultSeparator(l *lua.State) int {
	l.PushString(SEPARATOR)
	return 1
}

func parse(l *lua.State) int {
	p := lua.CheckString(l, 1)
	sep := SEPARATOR
	if !l.IsNone(2) {
		sep = lua.CheckString(l, 2)
	}
	return util.DeepPush(l, Parse(p, sep))
}

func getVarArgs(l *lua.State, from int) (vargs []string) {
	for i := from; i <= l.Top(); i++ {
		s, ok := l.ToString(i)
		if !ok {
			lua.Errorf(l, "invalid type, string expected")
			panic("unreachable")
		}
		vargs = append(vargs, s)
	}
	return
}

func Parse(pth, sep string) map[string]string {
	if strings.HasSuffix(pth, sep) {
		pth = pth[0 : len(pth)-1]
	}
	lastIndex := strings.LastIndex(pth, sep)
	if lastIndex == -1 {
		// no separator
		return map[string]string{
			"parent":    "", // no parent
			"base_name": pth,
		}
	}
	parent := pth[0 : lastIndex+1] // include sep
	baseName := pth[lastIndex+1:]  // don't include sep
	return map[string]string{
		"parent":    parent,
		"base_name": baseName,
	}
}

func join(l *lua.State) int {
	sep := lua.CheckString(l, 1)
	parts := getVarArgs(l, 2)
	l.PushString(Join(sep, parts...))
	return 1
}

func Join(sep string, parts ...string) string {
	s := ""
	for i, part := range parts {
		s += part // append it
		isLast := i == len(parts)-1
		if !isLast {
			// check if ends with sep, if not, add it
			if !strings.HasSuffix(part, sep) {
				s += sep
			}
		}
	}
	return s
}

func IsHidden(pth, sep, prefix string) bool {
	for pth != "" {
		parsed := Parse(pth, sep)
		if strings.HasPrefix(parsed["base_name"], prefix) {
			return true
		}
		pth = parsed["parent"]
	}
	return false
}

func isHidden(l *lua.State) int {
	p := lua.CheckString(l, 1)
	sep := SEPARATOR
	if !l.IsNone(2) {
		sep = lua.CheckString(l, 2)
	}
	prefix := HiddenPrefix
	if !l.IsNone(3) {
		prefix = lua.CheckString(l, 3)
	}
	l.PushBoolean(IsHidden(p, sep, prefix))
	return 1
}
