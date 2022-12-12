package regexp

import (
	"regexp"

	"github.com/Shopify/go-lua"
	"github.com/treeverse/lakefs/pkg/actions/lua/util"
)

// Open exposes the regexp functions to Lua code in the `regexp`
// namespace.
func Open(l *lua.State) {
	reOpen := func(l *lua.State) int {
		lua.NewLibrary(l, regexpLibrary)
		return 1
	}
	lua.Require(l, "regexp", reOpen, false)
	l.Pop(1)
}

var regexpLibrary = []lua.RegistryFunction{
	{Name: "match", Function: match},
	{Name: "quotemeta", Function: quoteMeta},
	{Name: "compile", Function: compile},
}

func match(l *lua.State) int {
	pattern := lua.CheckString(l, 1)
	s := lua.CheckString(l, 2)

	matched, err := regexp.MatchString(pattern, s)
	if err != nil {
		lua.Errorf(l, err.Error())
		panic("unreachable")
	}

	l.PushBoolean(matched)
	return 1
}

func quoteMeta(l *lua.State) int {
	s := lua.CheckString(l, 1)
	quoted := regexp.QuoteMeta(s)
	l.PushString(quoted)
	return 1
}

func compile(l *lua.State) int {
	expr := lua.CheckString(l, 1)
	re, err := regexp.Compile(expr)
	if err != nil {
		lua.Errorf(l, err.Error())
		panic("unreachable")
	}

	l.NewTable()
	for name, goFn := range regexpFunc {
		// -1: tbl
		l.PushGoFunction(goFn(re))
		// -1: fn, -2:tbl
		l.SetField(-2, name)
	}

	return 1
}

var regexpFunc = map[string]func(*regexp.Regexp) lua.Function{
	"findAll":         reFindAll,
	"findAllSubmatch": reFindAllSubmatch,
	"find":            reFind,
	"findSubmatch":    reFindSubmatch,
}

func reFindAll(re *regexp.Regexp) lua.Function {
	return func(l *lua.State) int {
		s := lua.CheckString(l, 1)
		n := lua.CheckInteger(l, 2)
		all := re.FindAllString(s, n)
		return util.DeepPush(l, all)
	}
}

func reFindAllSubmatch(re *regexp.Regexp) lua.Function {
	return func(l *lua.State) int {
		s := lua.CheckString(l, 1)
		n := lua.CheckInteger(l, 2)
		allSubmatch := re.FindAllStringSubmatch(s, n)
		return util.DeepPush(l, allSubmatch)
	}
}

func reFind(re *regexp.Regexp) lua.Function {
	return func(l *lua.State) int {
		s := lua.CheckString(l, 1)
		all := re.FindString(s)
		return util.DeepPush(l, all)
	}
}

func reFindSubmatch(re *regexp.Regexp) lua.Function {
	return func(l *lua.State) int {
		s := lua.CheckString(l, 1)
		allSubmatch := re.FindStringSubmatch(s)
		return util.DeepPush(l, allSubmatch)
	}
}
