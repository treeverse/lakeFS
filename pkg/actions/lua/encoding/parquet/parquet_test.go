package parquet_test

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/treeverse/lakefs/pkg/actions/lua/encoding/parquet"

	"github.com/treeverse/lakefs/pkg/actions/lua/encoding/json"

	"github.com/Shopify/go-lua"

	lualibs "github.com/treeverse/lakefs/pkg/actions/lua"
)

var parquetSchemaRead = `
parquet = require("encoding/parquet")
schema = parquet.get_schema(paruqet_content)

for _, col in pairs(schema) do
	print(col.name .. "\t" .. col.type)
end
`

var expected = `geoname_id	BYTE_ARRAY
name	BYTE_ARRAY
ascii_name	BYTE_ARRAY
alternate_names	BYTE_ARRAY
feature_class	BYTE_ARRAY
feature_code	BYTE_ARRAY
country_code	BYTE_ARRAY
country_name_en	BYTE_ARRAY
population	INT32
timezone	BYTE_ARRAY
modification_date	INT32
label_en	BYTE_ARRAY
coordinates	BYTE_ARRAY
`

func TestOpen(t *testing.T) {
	out := bytes.Buffer{}
	l := lua.NewState()
	lualibs.OpenSafe(l, context.Background(), &out)
	parquet.Open(l)
	json.Open(l)

	parquetBytes, err := os.ReadFile("testdata/000.snappy.parquet")
	if err != nil {
		t.Fatal(err)
	}
	l.PushString(string(parquetBytes))
	l.SetGlobal("paruqet_content")

	err = lua.DoString(l, parquetSchemaRead)
	if err != nil {
		t.Fatal(err)
	}

	printed := out.String()
	if printed != expected {
		t.Fatalf("got unexpected schema: %s", printed)
	}
}
