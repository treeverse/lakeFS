package md5

import (
	"crypto/md5" //#nosec
	"fmt"

	"github.com/Shopify/go-lua"
)

func Open(l *lua.State) {
	md5Open := func(l *lua.State) int {
		lua.NewLibrary(l, md5Library)
		return 1
	}
	lua.Require(l, "crypto/md5", md5Open, false)
	l.Pop(1)
}

var md5Library = []lua.RegistryFunction{
	{Name: "digest", Function: digest},
}

func digest(l *lua.State) int {
	data := lua.CheckString(l, 1)
	sum := md5.Sum([]byte(data)) //#nosec
	l.PushString(fmt.Sprintf("%x", sum))
	return 1
}
