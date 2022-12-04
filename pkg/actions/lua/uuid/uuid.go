package uuid

import (
	"github.com/Shopify/go-lua"
	"github.com/pborman/uuid"
)

var library = []lua.RegistryFunction{
	{"new", func(l *lua.State) int { l.PushString(uuid.New()); return 1 }},
}

func Open(l *lua.State) {
	require := func(l *lua.State) int {
		lua.NewLibrary(l, library)
		return 1
	}
	lua.Require(l, "uuid", require, false)
	l.Pop(1)
}
