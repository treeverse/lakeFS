package json

import (
	"bytes"
	"encoding/json"

	"github.com/Shopify/go-lua"
	"github.com/treeverse/lakefs/pkg/actions/lua/util"
)

func Open(l *lua.State) {
	jsonOpen := func(l *lua.State) int {
		lua.NewLibrary(l, jsonLibrary)
		return 1
	}
	lua.Require(l, "encoding/json", jsonOpen, false)
	l.Pop(1)
}

var jsonLibrary = []lua.RegistryFunction{
	{Name: "marshal", Function: jsonMarshal},
	{Name: "unmarshal", Function: jsonUnmarshal},
}

func check(l *lua.State, err error) {
	if err != nil {
		lua.Errorf(l, err.Error())
		panic("unreachable")
	}
}

func jsonMarshal(l *lua.State) int {
	var t interface{}
	var err error
	if !l.IsNil(1) {
		t, err = util.PullTable(l, 1)
		check(l, err)
	}
	var buf bytes.Buffer
	e := json.NewEncoder(&buf)
	e.SetIndent("", "  ")
	err = e.Encode(t)
	check(l, err)
	l.PushString(buf.String())
	return 1
}

func jsonUnmarshal(l *lua.State) int {
	payload := lua.CheckString(l, 1)
	var output interface{}
	check(l, json.Unmarshal([]byte(payload), &output))
	return util.DeepPush(l, output)
}
