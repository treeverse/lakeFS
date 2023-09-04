package yaml

import (
	"bytes"

	"github.com/Shopify/go-lua"
	"github.com/treeverse/lakefs/pkg/actions/lua/util"
	"gopkg.in/yaml.v3"
)

func Open(l *lua.State) {
	yamlOpen := func(l *lua.State) int {
		lua.NewLibrary(l, yamlLibrary)
		return 1
	}
	lua.Require(l, "encoding/yaml", yamlOpen, false)
	l.Pop(1)
}

var yamlLibrary = []lua.RegistryFunction{
	{Name: "marshal", Function: yamlMarshal},
	{Name: "unmarshal", Function: yamlUnmarshal},
}

func check(l *lua.State, err error) {
	if err != nil {
		lua.Errorf(l, err.Error())
		panic("unreachable")
	}
}

func yamlMarshal(l *lua.State) int {
	var t interface{}
	var err error
	if !l.IsNil(1) {
		t, err = util.PullTable(l, 1)
		check(l, err)
	}
	var buf bytes.Buffer
	e := yaml.NewEncoder(&buf)
	err = e.Encode(t)
	check(l, err)
	l.PushString(buf.String())
	return 1
}

func yamlUnmarshal(l *lua.State) int {
	payload := lua.CheckString(l, 1)
	var output interface{}
	check(l, yaml.Unmarshal([]byte(payload), &output))
	return util.DeepPush(l, output)
}
