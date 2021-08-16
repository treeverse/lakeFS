package parser_test

import (
	"fmt"
	"os"
	"sort"

	"github.com/cockroachdb/pebble/sstable"
)

const (
	KeyLength = 100
)

type key string

func main() {

	//TODO: decide on the logic according to which we need to create the input files. What test cases I'm writing
	//jsonPaths := []string{}
	//inputSizesKb := []int{1000, 10000, 100000}
	//var path string
	//for _, size := range inputSizesKb {
	//	path = generateSortedData(size)
	//	jsonPaths = append(jsonPaths, path)
	//}

	// TODO: populate with the final input files.`
	sampleData := []string{"cat", "dog", "horse", "frog"}
	sort.Strings(sampleData)
	writePebbleSst(sampleData)
}

func writePebbleSst(data []string) {
	file, _ := os.Create("test-data/testfile")

	writer := sstable.NewWriter(file, sstable.WriterOptions{
		Compression: sstable.SnappyCompression,
		//TODO: confirm that I don't need those properties
		//TablePropertyCollectors: []func() sstable.TablePropertyCollector{NewStaticCollector(props)},
	})

	for _, v := range data {
		if err := writer.Set([]byte(v), []byte("cur_value")); err != nil {
			fmt.Errorf("setting key and value: %w", err)
		}
	}
	writer.Close()
}

func generateSortedData(size int) string {
	numLines := size / KeyLength
	for i := 0; i < numLines; i++ {

	}
	return ""
}
