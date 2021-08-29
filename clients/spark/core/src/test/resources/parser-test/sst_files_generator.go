package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"

	fuzz "github.com/google/gofuzz"

	"github.com/cockroachdb/pebble/sstable"
	nanoid "github.com/matoous/go-nanoid/v2"
)

const (
	DefaultKeySizeBytes   = 100
	DefaultValueSizeBytes = 5
	MbToBytes             = 1024 * 1024
	KbToBytes             = 1024
	TwoLevelIndexType     = 2
	FuzzerSeed            = 20
	// The max file size that can be written by graveler (pkg/config/config.go)
	DefaultCommittedPermanentMaxRangeSizeMb = 20
)

type Entry struct {
	Key   string
	Value string
}

func main() {
	generateTwoLevelIdxSst(10 * KbToBytes)
	fuzzSstContents()
	fuzzWriterOptions()
	sstsWithUnsupportedWriterOptions()
	generateLargeSsts() //TODO: accelerate the scala test using large file as an input
}

func sstsWithUnsupportedWriterOptions() {
	sizeBytes := 10 * KbToBytes
	keys := generateSortedSlice(sizeBytes)

	// Checksum specifies which checksum to use. lakeFS supports parsing sstables with crc checksum.
	// ChecksumTypeXXHash and ChecksumTypeNone are unsupported and therefore not tested.
	writerOptions := sstable.WriterOptions{
		Compression: sstable.SnappyCompression,
		Checksum:    sstable.ChecksumTypeXXHash64,
	}
	writeTestInputFiles(keys, func() (string, error) { return nanoid.New(DefaultValueSizeBytes) }, sizeBytes,
		"checksum.type.xxHash64", writerOptions)

	//// TableFormat specifies the format version for writing sstables. The default
	//// is TableFormatRocksDBv2 which creates RocksDB compatible sstables. this is the only format supported by lakeFS.
	writerOptions = sstable.WriterOptions{
		Compression: sstable.SnappyCompression,
		TableFormat: sstable.TableFormatLevelDB,
	}
	writeTestInputFiles(keys, func() (string, error) { return nanoid.New(DefaultValueSizeBytes) }, sizeBytes,
		"table.format.leveldb", writerOptions)
}

func fuzzWriterOptions() {
	sizeBytes := 50 * KbToBytes
	keys := generateSortedSlice(sizeBytes)

	// BlockRestartInterval is the number of keys between restart points
	// for delta encoding of keys.
	// The default value is 16.
	blockRestartInterval := rand.Intn(100)
	writerOptions := sstable.WriterOptions{
		Compression:          sstable.SnappyCompression,
		BlockRestartInterval: blockRestartInterval,
	}
	fmt.Printf("BlockRestartInterval %d...\n", blockRestartInterval)
	writeTestInputFiles(keys, func() (string, error) { return nanoid.New(DefaultValueSizeBytes) }, sizeBytes,
		"fuzz.block.restart.interval", writerOptions)

	// BlockSize is the target uncompressed size in bytes of each table block.
	// The default value is 4096.
	blockSize := rand.Intn(9000)
	writerOptions = sstable.WriterOptions{
		Compression: sstable.SnappyCompression,
		BlockSize:   blockSize,
	}
	fmt.Printf("BlockSize %d...\n", blockSize)
	writeTestInputFiles(keys, func() (string, error) { return nanoid.New(DefaultValueSizeBytes) }, sizeBytes,
		"fuzz.block.size", writerOptions)

	//go func() {
	// BlockSizeThreshold finishes a block if the block size is larger than the
	// specified percentage of the target block size and adding the next entry
	// would cause the block to be larger than the target block size.
	//
	// The default value is 90
	blockSizeThreshold := rand.Intn(100)
	writerOptions = sstable.WriterOptions{
		Compression:        sstable.SnappyCompression,
		BlockSizeThreshold: blockSizeThreshold,
	}
	fmt.Printf("BlockSizeThreshold %d...\n", blockSizeThreshold)
	writeTestInputFiles(keys, func() (string, error) { return nanoid.New(DefaultValueSizeBytes) }, sizeBytes,
		"fuzz.block.size.threshold", writerOptions)
}

func generateLargeSsts() {
	sizeBytes := DefaultCommittedPermanentMaxRangeSizeMb * MbToBytes
	keys := generateSortedSlice(sizeBytes)
	writerOptions := sstable.WriterOptions{
		Compression: sstable.SnappyCompression,
	}
	writeTestInputFiles(keys, func() (string, error) { return nanoid.New(DefaultValueSizeBytes) }, sizeBytes,
		"max.size.lakefs.file", writerOptions)
}

func fuzzSstContents() {
	fileSizes := []int{KbToBytes, 2 * KbToBytes, 5 * KbToBytes, 8 * KbToBytes}
	for _, size := range fileSizes {
		sizeBytes := size
		testFileName := fmt.Sprintf("fuzz.contents.%d", sizeBytes)
		keys := generateSortedSliceWithFuzzing(sizeBytes)

		writerOptions := sstable.WriterOptions{
			Compression: sstable.SnappyCompression,
			//TODO: confirm that I don't need those properties
			//TablePropertyCollectors: []func() sstable.TablePropertyCollector{NewStaticCollector(props)},
		}
		writeTestInputFiles(keys, func() (string, error) {
			f := fuzz.NewWithSeed(FuzzerSeed) //TODO: this is inefficient to keep generating a fuzzer on each value creation
			var val string
			f.Fuzz(&val)
			return val, nil
		}, sizeBytes, testFileName, writerOptions)
	}
}

func generateSortedSliceWithFuzzing(size int) []string {
	numRecords := size / rand.Intn(size/10)
	fmt.Printf("numRecords %d...\n", numRecords)

	slice := make([]string, 0, numRecords)
	f := fuzz.NewWithSeed(FuzzerSeed)
	for i := 0; i < numRecords; i++ {
		var key string
		f.Fuzz(&key)
		if key == "" { // TODO: (Tals) remove after resolving https://github.com/treeverse/lakeFS/issues/2419
			continue
		}
		slice = append(slice, key)
	}
	sort.Strings(slice)
	return slice
}

func generateTwoLevelIdxSst(sizeBytes int) {
	keys := generateSortedSlice(sizeBytes)

	writerOptions := sstable.WriterOptions{
		Compression:    sstable.SnappyCompression,
		IndexBlockSize: 5, // setting the index block size target to a small number to make 2-level index get enabled
		//TODO: confirm that I don't need those properties
		//TablePropertyCollectors: []func() sstable.TablePropertyCollector{NewStaticCollector(props)},
	}
	writeTestInputFiles(keys, func() (string, error) { return nanoid.New(DefaultValueSizeBytes) }, sizeBytes,
		"two.level.idx", writerOptions)
}

func generateSortedSlice(size int) []string {
	numLines := size / DefaultKeySizeBytes
	slice := make([]string, 0, numLines)
	for i := 0; i < numLines; i++ {
		key, err := nanoid.New(DefaultKeySizeBytes)
		if err != nil {
			panic(err)
		}
		slice = append(slice, key)
	}
	sort.Strings(slice)
	return slice
}

// Writes sst and json files that will be used to unit test the sst parser.
// The sst file is the test input, and the json file encapsulates the expected parser output.
func writeTestInputFiles(keys []string, genValue func() (string, error), size int, name string,
	writerOptions sstable.WriterOptions) {
	fmt.Printf("Generate %s...\n", name)
	sstFile, err := os.Create(name + ".sst")
	if err != nil {
		panic(err)
	}

	expectedContents := make([]Entry, 0, len(keys))

	writer := sstable.NewWriter(sstFile, writerOptions)
	defer func() {
		_ = writer.Close()
		//TODO: clean this up
		//metadata, _ := writer.Metadata()
		//idxType := metadata.Properties.IndexType
		//if twoLevelIdx && idxType != TwoLevelIndexType || !twoLevelIdx && idxType == TwoLevelIndexType {
		//	fmt.Printf("Unexpected index type, is 2-level index=%v but index type = %d\n", twoLevelIdx, idxType)
		//}
	}()

	for _, k := range keys {
		if writer.EstimatedSize() >= uint64(size) {
			break
		}
		v, err := genValue()
		if err != nil {
			panic(err)
		}
		if err := writer.Set([]byte(k), []byte(v)); err != nil {
			panic(fmt.Errorf("setting key and value: %w", err))
		}
		expectedContents = append(expectedContents, Entry{Key: k, Value: v})
	}
	saveExpectedContentsAsJson(expectedContents, name)
}

func saveExpectedContentsAsJson(contents []Entry, name string) {
	j, _ := json.Marshal(contents)
	ioutil.WriteFile(name+".json", j, os.ModePerm)
}
