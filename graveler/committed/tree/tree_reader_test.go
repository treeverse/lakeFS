package tree

import (
	"fmt"
	"testing"

	gr "github.com/treeverse/lakefs/graveler"

	"github.com/treeverse/lakefs/graveler/committed/tree/mocks"
)

func TestTreeReader(t *testing.T) {
	cache := mocks.NewCacheMap(100)
	treeRepo := InitTreesRepository(cache, &mocks.SstMgr{})
	scanner, err := treeRepo.NewIteratorFromTreeID(gr.TreeID("tree_e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"), gr.Key("_"))
	if err != nil {
		panic(err)
	}
	counter := 0
	for scanner.Next() {
		val := scanner.Value()
		fmt.Print(val)
		counter++
	}
	err = scanner.Err()
	if err != nil {
		panic(err)
	}
	fmt.Printf("number of records read: %d\n", counter)
}
