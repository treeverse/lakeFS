package json

import (
	"bytes"
	"encoding/json"
	"reflect"

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
		lua.Errorf(l, "%s", err.Error())
		panic("unreachable")
	}
}

func jsonMarshal(l *lua.State) int {
	var t interface{}
	var ot interface{}
	var err error
	if !l.IsNil(1) {
		t, err = util.PullTable(l, 1)
		check(l, err)
	}
	if !l.IsNoneOrNil(2) { // Options table
		ot, err = util.PullTable(l, 2)
		check(l, err)
	}
	var buf bytes.Buffer
	e := json.NewEncoder(&buf)
	var rot map[string]any
	if ot != nil {
		vot := reflect.ValueOf(ot)
		rot = vot.Interface().(map[string]any)
	}
	if rot != nil {
		pre, ind := fetchIndentProps(rot)
		e.SetIndent(pre, ind)
	}
	err = e.Encode(t)
	check(l, err)
	l.PushString(buf.String())
	return 1
}

func fetchIndentProps(rot map[string]any) (string, string) {
	prefix := ""
	indent := ""
	if p, ok := rot["prefix"]; ok {
		prefix = p.(string)
	}
	if i, ok := rot["indent"]; ok {
		indent = i.(string)
	}
	return prefix, indent
}

func jsonUnmarshal(l *lua.State) int {
	payload := lua.CheckString(l, 1)
	var output interface{}
	check(l, json.Unmarshal([]byte(payload), &output))
	return util.DeepPush(l, output)
}
