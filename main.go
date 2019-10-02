package main

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type Branch string

func main() {
	fdb.MustAPIVersion(600)

	db := fdb.MustOpenDefault()

	dir, err := directory.CreateOrOpen(db, []string{"md_index"}, nil)
	if err != nil {
		panic(err)
	}
	workspace := dir.Sub("workspace")

	db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		for i := 0; i < 10; i++ {
			t := workspace.Pack(tuple.Tuple{"client_id", "repo", "master", fmt.Sprintf("key-%d", i)})
			t1 := workspace.Pack(tuple.Tuple{"client_id", "repo", "feature", fmt.Sprintf("key-%d", i)})
			t2 := workspace.Pack(tuple.Tuple{"client_id", "repo", "masterr", fmt.Sprintf("key-%d", i)})
			fmt.Printf("writing key: %s\n", t)
			tx.Set(t, []byte(fmt.Sprintf("value-master-%d", i)))
			tx.Set(t1, []byte(fmt.Sprintf("value-feature-%d", i)))
			tx.Set(t2, []byte(fmt.Sprintf("value-masterrrrrr-%d", i)))
		}
		return nil, nil
	})

	start := workspace.Pack(tuple.Tuple{"client_id", "repo"})
	end := workspace.Pack(tuple.Tuple{"client_id", "repo", 0xFF})
	fmt.Printf("start: %s, end: %s\n", start, end)

	db.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		result := tx.GetRange(fdb.KeyRange{start, end}, fdb.RangeOptions{}).Iterator()
		for result.Advance() {
			kv := result.MustGet()
			keyTuple, err := workspace.Unpack(kv.Key)
			if err != nil {
				panic(err)
			}
			fmt.Printf("key tuple: %v\n", keyTuple)
		}

		return nil, nil
	})
}
