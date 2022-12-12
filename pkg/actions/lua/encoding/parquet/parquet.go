package parquet

import (
	"github.com/Shopify/go-lua"
	"github.com/treeverse/lakefs/pkg/actions/lua/util"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
)

func Open(l *lua.State) {
	parquetOpen := func(l *lua.State) int {
		lua.NewLibrary(l, parquetLibrary)
		return 1
	}
	lua.Require(l, "encoding/parquet", parquetOpen, false)
	l.Pop(1)
}

var parquetLibrary = []lua.RegistryFunction{
	{Name: "get_schema", Function: getSchema},
}

func check(l *lua.State, err error) {
	if err != nil {
		lua.Errorf(l, err.Error())
		panic("unreachable")
	}
}

func getSchema(l *lua.State) int {
	payload := lua.CheckString(l, 1)
	bin := []byte(payload)
	t := struct{}{}
	fp, err := buffer.NewBufferFile(bin)
	check(l, err)
	r, err := reader.NewParquetReader(fp, &t, 1)
	check(l, err)
	output := make([]map[string]string, 0)
	for i, elem := range r.Footer.GetSchema() {
		if i == 0 {
			continue // root element
		}
		if elem.Type == nil {
			output = append(output, map[string]string{"name": elem.Name, "type": "N/A"})
		} else {
			output = append(output, map[string]string{"name": elem.Name, "type": elem.Type.String()})
		}
	}
	return util.DeepPush(l, output)
}
