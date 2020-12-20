package tree

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/treeverse/lakefs/graveler"

	"github.com/treeverse/lakefs/graveler/committed/tree/mocks"
)

const currentTreeID = graveler.TreeID("tree_e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")

func TestTreeReader(t *testing.T) {
	cache := mocks.NewCacheMap(100)
	treeRepo := InitTreesRepository(cache, &mocks.SstMgr{})
	scanner, err := treeRepo.NewIteratorFromTreeID(currentTreeID, graveler.Key("_"))
	if err != nil {
		panic(err)
	}
	counter := 0
	for scanner.Next() {
		val := scanner.Value()
		if !bytes.Equal(val.Key, val.Value.Data) {
			t.Fatal("different values\n", string(val.Key), "\n", string(val.Value.Data), "\n")
		}
		counter++
	}
	err = scanner.Err()
	if err != nil {
		panic(err)
	}
	fmt.Printf("number of records read: %d\n", counter)
}

const NumOfReads = 500_000

func TestRandomRead(t *testing.T) {
	var keys []graveler.Key
	cache := mocks.NewCacheMap(100)
	treeRepo := InitTreesRepository(cache, &mocks.SstMgr{})
	f, err := os.Open(`testdata/test_input.csv`)
	if err != nil {
		t.Fatal("open csv failed: ", err)
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		key := scanner.Text()
		keys = append(keys, graveler.Key(key))
	}
	if scanner.Err() != nil {
		panic(scanner.Err())
	}
	minPos := int32(math.MaxInt32)
	var misses int
	start := time.Now()
	for i := 0; i < NumOfReads; i++ {
		pos := rand.Int31n(int32(len(keys)))
		k := keys[pos]
		x := treeRepo
		data, err := x.GetValue(currentTreeID, k)
		if err != nil {
			if err == ErrPathBiggerThanMaxPath {
				misses++
				if minPos > pos {
					minPos = pos
				}
				fmt.Printf("too big, pos %d , itertion %d \n", pos, i)
				continue
			} else {
				t.Fatal("get value error ", err)
			}
		}
		if !bytes.Equal(data.Data, k) {
			t.Fatal(string(data.Data), "\n", string(k))
		}

	}
	elapsed := time.Since(start)
	fmt.Printf("queries took took %s\n", elapsed)
	fmt.Printf(" min pos %d, misses %d\n", minPos, misses)
}
