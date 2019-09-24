package main

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

func main() {
	fdb.MustAPIVersion(600)

	db := fdb.MustOpenDefault()
	dir, err := directory.CreateOrOpen(db, []string{"md_index"}, nil)
	if err != nil {
		panic(err)
	}
	workspace := dir.Sub("workspace")
	ret, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		k := workspace.Pack(tuple.Tuple{"foor", "bar", 3})
		// tr.Set(k, []byte("hello world"))
		return tr.Get(k).MustGet(), nil
	})
	if e != nil {
		panic(e)
	}
	fmt.Printf("got back value: %s\n", ret)
}
