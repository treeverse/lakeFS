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
	FuzzerSeed            = 50
	// DefaultCommittedPermanentMaxRangeSizeMb - the max file size that can be written by graveler (pkg/config/config.go)
	DefaultCommittedPermanentMaxRangeSizeMb = 20
)

func getDefaultUserProperties() map[string]string {
	return map[string]string{
		"user_prop_1": "val1",
		"user_prop_2": "val2",
	}
}

type Entry struct {
	Key   string
	Value string
}

func main() {
	generateTwoLevelIdxSst()
	fuzzSstContents()
	fuzzWriterOptions()
	sstsWithUnsupportedWriterOptions()
	generateLargeSsts()
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

	// TableFormat specifies the format version for writing sstables. The default
	// is TableFormatRocksDBv2 which creates RocksDB compatible sstables. this is the only format supported by lakeFS.
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

	// Use fuzzing to define sstable user properties.
	f := fuzz.NewWithSeed(FuzzerSeed)
	userProps := map[string]string{}
	for i := 1; i <= 20; i++ {
		var propKey string
		var propVal string
		f.Fuzz(&propKey)
		f.Fuzz(&propVal)
		userProps[propKey] = propVal
		fmt.Printf("user property %d, key=%s, val=%s\n", i, propKey, propVal)
	}
	writerOptions := sstable.WriterOptions{
		Compression:             sstable.SnappyCompression,
		TablePropertyCollectors: []func() sstable.TablePropertyCollector{NewStaticCollector(userProps)},
	}
	writeTestInputFiles(keys, func() (string, error) { return nanoid.New(DefaultValueSizeBytes) }, sizeBytes,
		"fuzz.table.properties", writerOptions)

	// BlockRestartInterval is the number of keys between restart points
	// for delta encoding of keys.
	// The default value is 16.
	blockRestartInterval := rand.Intn(100)
	writerOptions = sstable.WriterOptions{
		Compression:             sstable.SnappyCompression,
		TablePropertyCollectors: []func() sstable.TablePropertyCollector{NewStaticCollector(getDefaultUserProperties())},
		BlockRestartInterval:    blockRestartInterval,
	}
	fmt.Printf("BlockRestartInterval %d...\n", blockRestartInterval)
	writeTestInputFiles(keys, func() (string, error) { return nanoid.New(DefaultValueSizeBytes) }, sizeBytes,
		"fuzz.block.restart.interval", writerOptions)

	// BlockSize is the target uncompressed size in bytes of each table block.
	// The default value is 4096.
	blockSize := rand.Intn(9000)
	writerOptions = sstable.WriterOptions{
		Compression:             sstable.SnappyCompression,
		TablePropertyCollectors: []func() sstable.TablePropertyCollector{NewStaticCollector(getDefaultUserProperties())},
		BlockSize:               blockSize,
	}
	fmt.Printf("BlockSize %d...\n", blockSize)
	writeTestInputFiles(keys, func() (string, error) { return nanoid.New(DefaultValueSizeBytes) }, sizeBytes,
		"fuzz.block.size", writerOptions)

	// BlockSizeThreshold finishes a block if the block size is larger than the
	// specified percentage of the target block size and adding the next entry
	// would cause the block to be larger than the target block size.
	//
	// The default value is 90
	blockSizeThreshold := rand.Intn(100)
	writerOptions = sstable.WriterOptions{
		Compression:             sstable.SnappyCompression,
		TablePropertyCollectors: []func() sstable.TablePropertyCollector{NewStaticCollector(getDefaultUserProperties())},
		BlockSizeThreshold:      blockSizeThreshold,
	}
	fmt.Printf("BlockSizeThreshold %d...\n", blockSizeThreshold)
	writeTestInputFiles(keys, func() (string, error) { return nanoid.New(DefaultValueSizeBytes) }, sizeBytes,
		"fuzz.block.size.threshold", writerOptions)
}

func generateLargeSsts() {
	sizeBytes := DefaultCommittedPermanentMaxRangeSizeMb * MbToBytes
	keys := generateSortedSlice(sizeBytes)
	writerOptions := sstable.WriterOptions{
		Compression:             sstable.SnappyCompression,
		TablePropertyCollectors: []func() sstable.TablePropertyCollector{NewStaticCollector(getDefaultUserProperties())},
	}
	writeTestInputFiles(keys, func() (string, error) { return nanoid.New(DefaultValueSizeBytes) }, sizeBytes,
		"max.size.lakefs.file", writerOptions)
}

// Generates sstables on multiple sizes and uses a fuzzer to generate their contents.
func fuzzSstContents() {
	var fileSizes []int
	for i := 0; i < 3; i++ {
		curMb := rand.Intn(DefaultCommittedPermanentMaxRangeSizeMb)
		curKb := rand.Intn(100)
		if curMb == 0 || curKb == 0 {
			continue
		}
		fileSizes = append(fileSizes, curMb*MbToBytes)
		fileSizes = append(fileSizes, curKb*KbToBytes)
	}

	for i, size := range fileSizes {
		sizeBytes := size
		testFileName := fmt.Sprintf("fuzz.contents.%d", i)
		keys := generateSortedSliceWithFuzzing(sizeBytes)

		writerOptions := sstable.WriterOptions{
			Compression:             sstable.SnappyCompression,
			TablePropertyCollectors: []func() sstable.TablePropertyCollector{NewStaticCollector(getDefaultUserProperties())},
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
	var generatedDataBytes int
	var slice []string
	f := fuzz.NewWithSeed(FuzzerSeed)
	// Keep generating keys until we reach the desired file size. This guarantees that the sstable writer will have
	// enough data to write an sstable of the desired size.
	for generatedDataBytes < size {
		var key string
		f.Fuzz(&key)
		if key == "" { // TODO: (Tals) remove after resolving https://github.com/treeverse/lakeFS/issues/2419
			continue
		}
		generatedDataBytes += len(key)
		slice = append(slice, key)
	}
	sort.Strings(slice)
	return slice
}

func generateTwoLevelIdxSst() {
	sizeBytes := 10 * KbToBytes
	keys := generateSortedSlice(sizeBytes)

	writerOptions := sstable.WriterOptions{
		Compression:             sstable.SnappyCompression,
		IndexBlockSize:          5, // setting the index block size target to a small number to make 2-level index get enabled
		TablePropertyCollectors: []func() sstable.TablePropertyCollector{NewStaticCollector(getDefaultUserProperties())},
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

// Writes sst and json files that will be used to unit test the sst block parser (clients/spark/core/src/main/scala/io/treeverse/jpebble/BlockParser.scala)
// The sst file is the test input, and the json file encapsulates the expected parser output.
func writeTestInputFiles(keys []string, genValue func() (string, error), size int, name string,
	writerOptions sstable.WriterOptions) {
	fmt.Printf("Generate %s of size %d bytes\n", name, size)
	sstFile, err := os.Create(name + ".sst")
	if err != nil {
		panic(err)
	}

	expectedContents := make([]Entry, 0, len(keys))
	writer := sstable.NewWriter(sstFile, writerOptions)
	defer func() {
		_ = writer.Close()
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

// Copied from pkg/graveler/sstable/collectors.go. lakeFS populates the following user properties for each sstable
// graveler writes: type (range/metarange), min_key, max_key, count, estimated_size_bytes
// staticCollector is an sstable.TablePropertyCollector that adds a map's values to the user
// property map.
type staticCollector struct {
	m map[string]string
}

func (*staticCollector) Add(key sstable.InternalKey, value []byte) error {
	return nil
}

func (*staticCollector) Name() string {
	return "static"
}

func (s *staticCollector) Finish(userProps map[string]string) error {
	for k, v := range s.m {
		userProps[k] = v
	}
	return nil
}

// NewStaticCollector returns an SSTable collector that will add the properties in m when
// writing ends.
func NewStaticCollector(m map[string]string) func() sstable.TablePropertyCollector {
	return func() sstable.TablePropertyCollector { return &staticCollector{m} }
}
