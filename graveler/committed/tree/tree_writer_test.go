package tree

import (
	"bufio"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/graveler"

	"github.com/treeverse/lakefs/graveler/committed/tree/mocks"
)

func TestSimpleTree(t *testing.T) {
	deleteSST("tesdata/")
	cache := mocks.NewCacheMap(100)
	trees := InitTreesRepository(cache, &mocks.SstMgr{})
	b := mocks.NewBatchCloser()
	tw := trees.NewTreeWriter(4_000, b)
	f, err := os.Open(`testdata/test_input.csv`)
	if err != nil {
		t.Fatal("open csv failed: ", err)
	}
	var lastKey string
	var lineCount int
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		key := scanner.Text()
		if key == lastKey {
			continue
		}
		lineCount++
		if key < lastKey {
			panic(" unsorted keys:" + lastKey + "  :  " + key)
		}
		lastKey = key
		val := graveler.Value{Data: []byte(key)}
		r := graveler.ValueRecord{
			Key:   graveler.Key(key),
			Value: &val,
		}
		if err := tw.WriteValue(r); err != nil {
			log.Fatal(err)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	_, err = tw.SaveTree()
	if err != nil {
		panic(err)
	}
}

func TestLargeTree(t *testing.T) {
	deleteSST("tesdata/")
	cache := mocks.NewCacheMap(100)
	trees := InitTreesRepository(cache, &mocks.SstMgr{})
	b := mocks.NewBatchCloser()
	tw := trees.NewTreeWriter(4_000_000, b) // regular file split will never happen wit large split factor
	f, err := os.Open(`testdata/test_input.csv`)
	if err != nil {
		t.Fatal("open csv failed: ", err)
	}
	var lastKey string
	var lineCount int
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		key := scanner.Text()
		if key == lastKey {
			continue
		}
		lineCount++
		if key < lastKey {
			panic(" unsorted keys:" + lastKey + "  :  " + key)
		}
		lastKey = key
		val := graveler.Value{Data: []byte(key)}
		r := graveler.ValueRecord{
			Key:   graveler.Key(key),
			Value: &val,
		}
		if err := tw.WriteValue(r); err != nil {
			log.Fatal(err)
		}
		if lineCount%50 == 0 {
			tw.ClosePart()
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	_, err = tw.SaveTree()
	if err != nil {
		panic(err)
	}
}

func deleteSST(directory string) {

	// Open the directory and read all its files.
	dirRead, _ := os.Open(directory)
	dirFiles, _ := dirRead.Readdir(0)
	// delet .sst and .json
	for _, fileName := range dirFiles {
		name := fileName.Name()
		if strings.HasSuffix(name, ".sst") || strings.HasSuffix(name, ".json") {
			fullPath := directory + name
			os.Remove(fullPath)
		}
	}
}
