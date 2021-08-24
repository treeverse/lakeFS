package main

import (
	"fmt"
	"os"
	"sort"

	"github.com/cockroachdb/pebble/sstable"
	nanoid "github.com/matoous/go-nanoid/v2"
)

const (
	DefaultKeySizeBytes = 100
	MbToBytes           = 1024 * 1024
	// The max file size that can be written by graveler (pkg/config/config.go)
	DefaultCommittedPermanentMaxRangeSizeMb = 20
)

func main() {

	writeTwoLevelIdxSst(10 * MbToBytes)

	// Test max sst file size
	//inputSizesBytes := []int{DefaultCommittedPermanentMaxRangeSizeMb}
	////inputSizesBytes := []int{1, 3, DefaultCommittedPermanentMaxRangeSizeMb}
	//for _, size := range inputSizesBytes {
	//	sizeBytes := size * MbToBytes
	//	sortedWords := generateSortedSlice(sizeBytes)
	//	writePebbleSst(sortedWords, sizeBytes, "max-size-file.sst")
	//}
}

func writeTwoLevelIdxSst(sizeBytes int) {
	keys := generateSortedSlice(sizeBytes)
	writePebbleSst(keys, sizeBytes, "2-level-idx.sst", true)
}

func generateSortedSlice(size int) []string {
	numLines := size / DefaultKeySizeBytes
	slice := make([]string, 0, numLines)
	for i := 0; i < numLines; i++ {
		// Generate a name.
		key, err := nanoid.New(DefaultKeySizeBytes)
		if err != nil {
			panic(err) //TODO: is this what I need to do here?
		}
		slice = append(slice, key)
	}
	//slice := generateStringsSlice(size, DefaultKeySizeBytes)
	sort.Strings(slice)
	return slice
}

func writePebbleSst(data []string, size int, name string, twoLevelIdx bool) {
	fmt.Printf("Generate %s...\n", name)
	file, err := os.Create(name)
	if err != nil {
		panic(err)
	}

	idxBlockSize := 4096 // Default index block size
	// https://github.com/cockroachdb/pebble/blob/2aba043dd4a270dfdd7731fedf99817164476618/sstable/options.go#L196
	if twoLevelIdx {
		// setting the index block size target to a small number to make 2-level index get enabled
		idxBlockSize = 100
	}
	writer := sstable.NewWriter(file, sstable.WriterOptions{
		Compression:    sstable.SnappyCompression,
		IndexBlockSize: idxBlockSize,
		//TODO: confirm that I don't need those properties
		//TablePropertyCollectors: []func() sstable.TablePropertyCollector{NewStaticCollector(props)},
	})

	defer func() {
		_ = writer.Close()
		metadata, _ := writer.Metadata()
		idxType := metadata.Properties.IndexType
		if twoLevelIdx && idxType != 2 || !twoLevelIdx && idxType == 2 {
			fmt.Printf("Unexpected index type, is 2-level index=%v but index type = %d", twoLevelIdx, idxType)
		}
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
