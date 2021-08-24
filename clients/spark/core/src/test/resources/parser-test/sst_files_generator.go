package main

import (
	"fmt"
	"os"
	"sort"

	"github.com/cockroachdb/pebble/sstable"
	nanoid "github.com/matoous/go-nanoid/v2"
)

const (
	KeyLength = 100
	MbToBytes = 1024 * 1024
	// The max file size that can be written by graveler (pkg/config/config.go)
	DefaultCommittedPermanentMaxRangeSizeMb = 20
)

func main() {

	generateTwoLevelIdxSst()

	inputSizesBytes := []int{DefaultCommittedPermanentMaxRangeSizeMb}
	//inputSizesBytes := []int{1, 3, DefaultCommittedPermanentMaxRangeSizeMb}
	for _, size := range inputSizesBytes {
		sizeBytes := size * MbToBytes
		sortedWords := generateSortedSlice(sizeBytes)
		writePebbleSst(sortedWords, sizeBytes)
	}

	// This is useful when there are very large index blocks, which generally occurs
	// with the usage of large keys. With large index blocks, the index blocks fight
	// the data blocks for block cache space and the index blocks are likely to be
	// re-read many times from the disk. The top level index, which has a much
	// smaller memory footprint, can be used to prevent the entire index block from
	// being loaded into the block cache.
	//twoLevelIndex bool
}

func generateTwoLevelIdxSst() {
	//keys := generateStringsSlice(1 * MbToBytes)

}

func generateStringsSlice(size int) []string {

}

func generateSortedSlice(size int) []string {
	numLines := size / KeyLength
	slice := make([]string, 0, numLines)
	for i := 0; i < numLines; i++ {
		// Generate a name.
		key, err := nanoid.New(KeyLength)
		if err != nil {
			panic(err) //TODO: is this what I need to do here?
		}
		slice = append(slice, key)
	}
	sort.Strings(slice)
	return slice
}

func writePebbleSst(data []string, size int) {
	fileName := fmt.Sprintf("large.file.%d-bytes.sst", size)
	fmt.Printf("Generate %s...\n", fileName)
	file, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}

	writer := sstable.NewWriter(file, sstable.WriterOptions{
		Compression: sstable.SnappyCompression,
		//TODO: confirm that I don't need those properties
		//TablePropertyCollectors: []func() sstable.TablePropertyCollector{NewStaticCollector(props)},
	})
	defer func() {
		metadata, _ := writer.Metadata()
		//idxType :=
		fmt.Printf("index type = %d", idxType)
		_ = writer.Close()
	}()

	for _, v := range data {
		if writer.EstimatedSize() >= uint64(size) {
			break
		}
		if err := writer.Set([]byte(v), []byte("some_value")); err != nil {
			panic(fmt.Errorf("setting key and value: %w", err))
		}
	}
}
