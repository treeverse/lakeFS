package main

import "github.com/apple/foundationdb/bindings/go/src/fdb"

func main() {
	fdb.MustAPIVersion(600)
}
