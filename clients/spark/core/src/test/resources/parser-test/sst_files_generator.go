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

	//generateTwoLevelIdxSst(10 * KbToBytes)

	//generateSstsWithContentsFuzzing()

	generateSstsWithWriterOptionsFuzzing()

	//generateLargeSsts() //TODO: accelerate the scala test using large file as an input

}

func generateSstsWithWriterOptionsFuzzing() {
	keys := generateSortedSlice(50 * KbToBytes)

	//// BlockRestartInterval is the number of keys between restart points
	//// for delta encoding of keys.
	////
	//// The default value is 16.
	//BlockRestartInterval int
	//
	//// BlockSize is the target uncompressed size in bytes of each table block.
	////
	//// The default value is 4096.
	//BlockSize int
	//
	//// BlockSizeThreshold finishes a block if the block size is larger than the
	//// specified percentage of the target block size and adding the next entry
	//// would cause the block to be larger than the target block size.
	////
	//// The default value is 90
	//BlockSizeThreshold int
	//
	//// Cache is used to cache uncompressed blocks from sstables.
	////
	//// The default is a nil cache.
	//Cache *cache.Cache
	//
	//// Comparer defines a total ordering over the space of []byte keys: a 'less
	//// than' relationship. The same comparison algorithm must be used for reads
	//// and writes over the lifetime of the DB.
	////
	//// The default value uses the same ordering as bytes.Compare.
	//Comparer *Comparer
	//
	//// Compression defines the per-block compression to use.
	////
	//// The default value (DefaultCompression) uses snappy compression.
	//Compression Compression
	//
	//// FilterPolicy defines a filter algorithm (such as a Bloom filter) that can
	//// reduce disk reads for Get calls.
	////
	//// One such implementation is bloom.FilterPolicy(10) from the pebble/bloom
	//// package.
	////
	//// The default value means to use no filter.
	//FilterPolicy FilterPolicy
	//
	//// FilterType defines whether an existing filter policy is applied at a
	//// block-level or table-level. Block-level filters use less memory to create,
	//// but are slower to access as a check for the key in the index must first be
	//// performed to locate the filter block. A table-level filter will require
	//// memory proportional to the number of keys in an sstable to create, but
	//// avoids the index lookup when determining if a key is present. Table-level
	//// filters should be preferred except under constrained memory situations.
	//FilterType FilterType
	//
	//// IndexBlockSize is the target uncompressed size in bytes of each index
	//// block. When the index block size is larger than this target, two-level
	//// indexes are automatically enabled. Setting this option to a large value
	//// (such as math.MaxInt32) disables the automatic creation of two-level
	//// indexes.
	////
	//// The default value is the value of BlockSize.
	//IndexBlockSize int
	//
	//// Merger defines the associative merge operation to use for merging values
	//// written with {Batch,DB}.Merge. The MergerName is checked for consistency
	//// with the value stored in the sstable when it was written.
	//MergerName string
	//
	//// TableFormat specifies the format version for writing sstables. The default
	//// is TableFormatRocksDBv2 which creates RocksDB compatible sstables. Use
	//// TableFormatLevelDB to create LevelDB compatible sstable which can be used
	//// by a wider range of tools and libraries.
	//TableFormat TableFormat
	//
	//// TablePropertyCollectors is a list of TablePropertyCollector creation
	//// functions. A new TablePropertyCollector is created for each sstable built
	//// and lives for the lifetime of the table.
	//TablePropertyCollectors []func() TablePropertyCollector
	//
	//// Checksum specifies which checksum to use.
	//Checksum ChecksumType
}

func generateLargeSsts() {
	// Test max sst file size
	//inputSizesBytes := []int{DefaultCommittedPermanentMaxRangeSizeMb}
	////inputSizesBytes := []int{1, 3, DefaultCommittedPermanentMaxRangeSizeMb}
	//for _, size := range inputSizesBytes {
	//	sizeBytes := size * MbToBytes
	//	sortedWords := generateSortedSlice(sizeBytes)
	//	writeTestInputFiles(sortedWords, sizeBytes, "max-size-file.sst")
	//}
}

func generateSstsWithContentsFuzzing() {
	fileSizes := []int{KbToBytes, 2 * KbToBytes, 5 * KbToBytes, 8 * KbToBytes}
	for _, size := range fileSizes {
		sizeBytes := size
		testFileName := fmt.Sprintf("fuzz.contents.%d", sizeBytes)
		keys := generateSortedSliceWithFuzzing(sizeBytes)
		writeTestInputFiles(keys, func() (string, error) {
			f := fuzz.NewWithSeed(FuzzerSeed) //TODO: this is inefficient to keep generating a fuzzer on each value creation
			var val string
			f.Fuzz(&val)
			return val, nil
		}, sizeBytes, testFileName, false)
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
	writeTestInputFiles(keys, func() (string, error) { return nanoid.New(DefaultValueSizeBytes) }, sizeBytes, "two.level.idx", true)
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
func writeTestInputFiles(keys []string, genValue func() (string, error), size int, name string, twoLevelIdx bool) {
	fmt.Printf("Generate %s...\n", name)
	sstFile, err := os.Create(name + ".sst")
	if err != nil {
		panic(err)
	}
	expectedContents := make([]Entry, 0, len(keys))

	idxBlockSize := 4096 // Default index block size
	// https://github.com/cockroachdb/pebble/blob/2aba043dd4a270dfdd7731fedf99817164476618/sstable/options.go#L196
	if twoLevelIdx {
		// setting the index block size target to a small number to make 2-level index get enabled
		idxBlockSize = 5
	}
	writer := sstable.NewWriter(sstFile, sstable.WriterOptions{
		Compression:    sstable.SnappyCompression,
		IndexBlockSize: idxBlockSize,
		//TODO: confirm that I don't need those properties
		//TablePropertyCollectors: []func() sstable.TablePropertyCollector{NewStaticCollector(props)},
	})

	defer func() {
		_ = writer.Close()
		metadata, _ := writer.Metadata()
		idxType := metadata.Properties.IndexType
		if twoLevelIdx && idxType != TwoLevelIndexType || !twoLevelIdx && idxType == TwoLevelIndexType {
			fmt.Printf("Unexpected index type, is 2-level index=%v but index type = %d\n", twoLevelIdx, idxType)
		}
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
