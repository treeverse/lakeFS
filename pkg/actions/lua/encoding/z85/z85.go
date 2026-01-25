package z85

import (
	"fmt"

	"github.com/Shopify/go-lua"
	"github.com/tilinna/z85"
)

func Open(l *lua.State) {
	open := func(l *lua.State) int {
		lua.NewLibrary(l, library)
		return 1
	}
	lua.Require(l, "encoding/z85", open, false)
	l.Pop(1)
}

var library = []lua.RegistryFunction{
	{Name: "decode", Function: decode},
	{Name: "decode_uuid", Function: decodeUUID},
}

func decode(l *lua.State) int {
	data := lua.CheckString(l, 1)
	dst := make([]byte, z85.DecodedLen(len(data)))
	_, err := z85.Decode(dst, []byte(data))
	if err != nil {
		lua.Errorf(l, "%s", err.Error())
		panic("unreachable")
	}
	l.PushString(string(dst))
	return 1
}

func decodeUUID(l *lua.State) int {
	data := lua.CheckString(l, 1)
	// Last 20 chars are the UUID, anything before is prefix
	encoded := data
	prefix := ""
	if len(data) > 20 {
		prefix = data[:len(data)-20]
		encoded = data[len(data)-20:]
	}
	dst := make([]byte, 16)
	_, err := z85.Decode(dst, []byte(encoded))
	if err != nil {
		lua.Errorf(l, "%s", err.Error())
		panic("unreachable")
	}
	// Format as UUID string
	uuid := fmt.Sprintf("%x-%x-%x-%x-%x", dst[0:4], dst[4:6], dst[6:8], dst[8:10], dst[10:16])
	// Return both UUID and prefix
	l.PushString(uuid)
	l.PushString(prefix)
	return 2
}
