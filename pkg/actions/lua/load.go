package lua

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Shopify/go-lua"
)

const (
	pathListSeparator = ';'
	defaultPath       = "./?.lua"
)

func findLoader(l *lua.State, name string) {
	var msg string
	if l.Field(lua.UpValueIndex(1), "searchers"); !l.IsTable(3) {
		lua.Errorf(l, "'package.searchers' must be a table")
	}
	for i := 1; ; i++ {
		if l.RawGetInt(3, i); l.IsNil(-1) {
			l.Pop(1)
			l.PushString(msg)
			lua.Errorf(l, "module '%s' not found: %s", name, msg)
		}
		l.PushString(name)
		if l.Call(1, 2); l.IsFunction(-2) {
			return
		} else if l.IsString(-2) {
			msg += lua.CheckString(l, -2)
		}
		l.Pop(2)
	}
}

func searcherPreload(l *lua.State) int {
	name := lua.CheckString(l, 1)
	l.Field(lua.RegistryIndex, "_PRELOAD")
	l.Field(-1, name)
	if l.IsNil(-1) {
		l.PushString(fmt.Sprintf("\n\tno field package.preload['%s']", name))
	}
	return 1
}

func createSearchersTable(l *lua.State) {
	searchers := []lua.Function{searcherPreload}
	l.CreateTable(len(searchers), 0)
	for i, s := range searchers {
		l.PushValue(-2)
		l.PushGoClosure(s, 1)
		l.RawSetInt(-2, i+1)
	}
}

func noEnv(l *lua.State) bool {
	l.Field(lua.RegistryIndex, "LUA_NOENV")
	b := l.ToBoolean(-1)
	l.Pop(1)
	return b
}

func setPath(l *lua.State, field, env, def string) {
	if path := os.Getenv(env); path == "" || noEnv(l) {
		l.PushString(def)
	} else {
		o := fmt.Sprintf("%c%c", pathListSeparator, pathListSeparator)
		n := fmt.Sprintf("%c%s%c", pathListSeparator, def, pathListSeparator)
		path = strings.Replace(path, o, n, -1)
		l.PushString(path)
	}
	l.SetField(-2, field)
}

var packageLibrary = []lua.RegistryFunction{
	{"loadlib", func(l *lua.State) int {
		_ = lua.CheckString(l, 1) // path
		_ = lua.CheckString(l, 2) // init
		l.PushNil()
		l.PushString("dynamic libraries not enabled; check your Lua installation")
		l.PushString("absent")
		return 3 // Return nil, error message, and where.
	}},
	{"searchpath", func(l *lua.State) int {
		_ = lua.CheckString(l, 1)
		_ = lua.CheckString(l, 2)
		_ = lua.OptString(l, 3, ".")
		_ = lua.OptString(l, 4, string(filepath.Separator))

		l.PushNil()
		l.PushString("searchpath not enabled; check your Lua installation")
		l.PushString("absent")
		return 3 // Return nil, error message, and where.
	}},
}

// PackageOpen opens the package library. Usually passed to Require.
func PackageOpen(l *lua.State) int {
	lua.NewLibrary(l, packageLibrary)
	createSearchersTable(l)
	l.SetField(-2, "searchers")
	setPath(l, "path", "LUA_PATH", defaultPath)
	l.PushString(fmt.Sprintf("%c\n%c\n?\n!\n-\n", filepath.Separator, pathListSeparator))
	l.SetField(-2, "config")
	lua.SubTable(l, lua.RegistryIndex, "_LOADED")
	l.SetField(-2, "loaded")
	lua.SubTable(l, lua.RegistryIndex, "_PRELOAD")
	l.SetField(-2, "preload")
	l.PushGlobalTable()
	l.PushValue(-2)
	lua.SetFunctions(l, []lua.RegistryFunction{{"require", func(l *lua.State) int {
		name := lua.CheckString(l, 1)
		l.SetTop(1)
		l.Field(lua.RegistryIndex, "_LOADED")
		l.Field(2, name)
		if l.ToBoolean(-1) {
			return 1
		}
		l.Pop(1)
		findLoader(l, name)
		l.PushString(name)
		l.Insert(-2)
		l.Call(2, 1)
		if !l.IsNil(-1) {
			l.SetField(2, name)
		}
		l.Field(2, name)
		if l.IsNil(-1) {
			l.PushBoolean(true)
			l.PushValue(-1)
			l.SetField(2, name)
		}
		return 1
	}}}, 1)
	l.Pop(1)
	return 1
}
