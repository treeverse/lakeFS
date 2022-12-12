package base64

import (
	"encoding/base64"

	"github.com/Shopify/go-lua"
)

func Open(l *lua.State) {
	base64Open := func(l *lua.State) int {
		lua.NewLibrary(l, base64Library)
		return 1
	}
	lua.Require(l, "encoding/base64", base64Open, false)
	l.Pop(1)
}

var base64Library = []lua.RegistryFunction{
	{Name: "encode", Function: encode},
	{Name: "decode", Function: decode},
	{Name: "urlEncode", Function: urlEncode},
	{Name: "urlDecode", Function: urlDecode},
}

func encode(l *lua.State) int {
	data := lua.CheckString(l, 1)
	l.PushString(base64.StdEncoding.EncodeToString([]byte(data)))
	return 1
}

func decode(l *lua.State) int {
	data := lua.CheckString(l, 1)
	decoded, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		lua.Errorf(l, err.Error())
		panic("unreachable")
	}
	l.PushString(string(decoded))
	return 1
}

func urlEncode(l *lua.State) int {
	data := lua.CheckString(l, 1)
	l.PushString(base64.RawURLEncoding.EncodeToString([]byte(data)))
	return 1
}

func urlDecode(l *lua.State) int {
	data := lua.CheckString(l, 1)
	decoded, err := base64.RawURLEncoding.DecodeString(data)
	if err != nil {
		lua.Errorf(l, err.Error())
		panic("unreachable")
	}
	l.PushString(string(decoded))
	return 1
}
