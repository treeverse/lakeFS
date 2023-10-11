package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"

	"github.com/cockroachdb/pebble/sstable"
	fuzz "github.com/google/gofuzz"
	nanoid "github.com/matoous/go-nanoid/v2"
)

const (
	DefaultKeySizeBytes   = 100
	DefaultValueSizeBytes = 5
	KiBToBytes            = 1 << 10
	MiBToBytes            = KiBToBytes << 10
	FuzzerSeed            = 50
	// DefaultCommittedPermanentMaxRangeSizeBytes - the max file size that can be written by graveler (pkg/config/config.go)
	DefaultCommittedPermanentMaxRangeSizeBytes = 20 * MiBToBytes
	DefaultSstSizeBytes                        = 50 * KiBToBytes
	DefaultTwoLevelSstSizeBytes                = 10 * KiBToBytes
	DefaultMaxSizeKiB                          = 100
	MagicLengthBytes                           = 8
	SharedPrefixBaseSize                       = 10
)

var DefaultUserProperties = map[string]string{
	"user_prop_1": "val1",
	"user_prop_2": "val2",
}

func newDefaultWriterOptions() sstable.WriterOptions {
	return sstable.WriterOptions{
		Compression:             sstable.SnappyCompression,
		TablePropertyCollectors: getDefaultTablePropertyCollectors(DefaultUserProperties),
	}
}

type Entry struct {
	Key   string
	Value string
}

func main() {
	writeTwoLevelIdxSst()
	writeMultiSizedSstsWithContentsFuzzing()
	writeSstsWithWriterOptionsFuzzing()
	writeSstsWithUnsupportedWriterOptions()
	writeLargeSsts()
	writeZeroRecordSst()
	writeEmptyFile()
	writeSstWithBadMagic()
}

func writeSstWithBadMagic() {
	testFileName := "bad.magic.mark"
	writerOptions := newDefaultWriterOptions()
	createTestInputFiles([]string{}, newGenerateFuzz(), 0, testFileName, writerOptions)
	// after creating a valid Pebble sstable that includes a correct magic, remove part of the bytes that compose the
	// magic to simulate an sstable with a bad magic.
	fi, err := os.Stat(testFileName + ".sst")
	if err != nil {
		panic(err)
	}
	err = os.Truncate(testFileName+".sst", fi.Size()-MagicLengthBytes/2)
	if err != nil {
		panic(err)
	}
}

func writeEmptyFile() {
	writer, err := os.Create("empty.file")
	if err != nil {
		panic(err)
	}
	defer func() {
		writer.Close()
	}()
}

func writeZeroRecordSst() {
	writerOptions := newDefaultWriterOptions()
	createTestInputFiles([]string{}, newGenerateFuzz(), 0, "zero.records.sst", writerOptions)
}

func newGenerateNanoid(size int) func() (string, error) {
	return func() (string, error) {
		return nanoid.New(size)
	}
}

func newGenerateFuzz() func() (string, error) {
	f := fuzz.NewWithSeed(FuzzerSeed)
	return func() (string, error) {
		var val string
		f.Fuzz(&val)
		return val, nil
	}
}

func newGenerateFuzzWithSharedPrefixes() func() (string, error) {
	f := fuzz.NewWithSeed(FuzzerSeed)
	var sharedPrefixBaseBytes [SharedPrefixBaseSize]byte
	f.Fuzz(&sharedPrefixBaseBytes)
	sharedPrefixBase := string(sharedPrefixBaseBytes[:])
	src := rand.NewSource(20211004)
	r := rand.New(src)
	return func() (string, error) {
		var suffix string
		f.Fuzz(&suffix)
		// play with the length of the shared prefix trying to create noisier input
		p := sharedPrefixBase[0:r.Intn(SharedPrefixBaseSize/2)+r.Intn(SharedPrefixBaseSize-SharedPrefixBaseSize/2)] + suffix
		return p, nil
	}
}

func writeSstsWithUnsupportedWriterOptions() {
	sizeBytes := DefaultSstSizeBytes
	generateNanoidKey := newGenerateNanoid(DefaultKeySizeBytes)
	keys := prepareSortedSlice(sizeBytes, generateNanoidKey)

	// Checksum specifies which checksum to use. lakeFS supports parsing sstables with crc checksum.
	// ChecksumTypeXXHash and ChecksumTypeNone are unsupported and therefore not tested.
	writerOptions := newDefaultWriterOptions()
	writerOptions.Checksum = sstable.ChecksumTypeXXHash64
	generateNanoidValue := newGenerateNanoid(DefaultValueSizeBytes)
	createTestInputFiles(keys, generateNanoidValue, sizeBytes,
		"checksum.type.xxHash64", writerOptions)

	// TableFormat specifies the format version for writing sstables. The default
	// is TableFormatRocksDBv2 which creates RocksDB compatible sstables. this is the only format supported by lakeFS.
	writerOptions = newDefaultWriterOptions()
	writerOptions.TableFormat = sstable.TableFormatLevelDB
	createTestInputFiles(keys, generateNanoidValue, sizeBytes, "table.format.leveldb",
		writerOptions)

	// Compression defines the per-block compression to use. lakeFS support Snappy or no compression.
	writerOptions = newDefaultWriterOptions()
	writerOptions.Compression = sstable.ZstdCompression
	createTestInputFiles(keys, generateNanoidValue, sizeBytes, "compression.type.zstd",
		writerOptions)
}

func writeSstsWithWriterOptionsFuzzing() {
	sizeBytes := DefaultSstSizeBytes
	generateNanoidKey := newGenerateNanoid(DefaultKeySizeBytes)
	keys := prepareSortedSlice(sizeBytes, generateNanoidKey)

	// Use fuzzing to define sstable user properties.
	generateFuzz := newGenerateFuzz()
	userProps := map[string]string{}
	const numOfUserDefinedProps = 20
	for i := 1; i <= numOfUserDefinedProps; i++ {
		propKey, _ := generateFuzz()
		propVal, _ := generateFuzz()
		userProps[propKey] = propVal
		fmt.Printf("user property %d, key=%s, val=%s\n", i, propKey, propVal)
	}
	writerOptions := newDefaultWriterOptions()
	writerOptions.TablePropertyCollectors = []func() sstable.TablePropertyCollector{NewStaticCollector(userProps)}
	generateNanoidValue := newGenerateNanoid(DefaultValueSizeBytes)
	createTestInputFiles(keys, generateNanoidValue, sizeBytes, "fuzz.table.properties", writerOptions)

	// BlockRestartInterval is the number of keys between restart points
	// for delta encoding of keys.
	// The default value is 16.
	src1 := rand.NewSource(5092021)
	blockRestartInterval := 1 + rand.New(src1).Intn(100)
	writerOptions = newDefaultWriterOptions()
	writerOptions.BlockRestartInterval = blockRestartInterval
	fmt.Printf("BlockRestartInterval %d...\n", blockRestartInterval)
	createTestInputFiles(keys, generateNanoidValue, sizeBytes, "fuzz.block.restart.interval", writerOptions)

	// BlockSize is the target uncompressed size in bytes of each table block.
	// The default value is 4096.
	src2 := rand.NewSource(881989)
	blockSize := 1 + rand.New(src2).Intn(9000)
	writerOptions = newDefaultWriterOptions()
	writerOptions.BlockSize = blockSize
	fmt.Printf("BlockSize %d...\n", blockSize)
	createTestInputFiles(keys, generateNanoidValue, sizeBytes, "fuzz.block.size", writerOptions)

	// BlockSizeThreshold finishes a block if the block size is larger than the
	// specified percentage of the target block size and adding the next entry
	// would cause the block to be larger than the target block size.
	//
	// The default value is 90
	src3 := rand.NewSource(123432)
	blockSizeThreshold := 1 + rand.New(src3).Intn(100)
	writerOptions = newDefaultWriterOptions()
	writerOptions.BlockSizeThreshold = blockSizeThreshold
	fmt.Printf("BlockSizeThreshold %d...\n", blockSizeThreshold)
	createTestInputFiles(keys, generateNanoidValue, sizeBytes, "fuzz.block.size.threshold", writerOptions)
}

func writeLargeSsts() {
	sizeBytes := DefaultCommittedPermanentMaxRangeSizeBytes
	generateNanoidKey := newGenerateNanoid(DefaultKeySizeBytes)
	keys := prepareSortedSlice(sizeBytes, generateNanoidKey)
	writerOptions := newDefaultWriterOptions()
	generateNanoidValue := newGenerateNanoid(DefaultValueSizeBytes)
	createTestInputFiles(keys, generateNanoidValue, sizeBytes, "max.size.lakefs.file", writerOptions)
}

func writeMultiSizedSstsWithContentsFuzzing() {
	const numOfFilesToGenerate = 6
	src := rand.NewSource(980433)
	r := rand.New(src)
	for i := 0; i < numOfFilesToGenerate; i++ {
		var curSize int
		var keyFuzzFunc func() (string, error)
		testFileName := fmt.Sprintf("fuzz.contents.%d", i)
		if i%2 == 0 {
			curSize = (1 + r.Intn(DefaultCommittedPermanentMaxRangeSizeBytes/MiBToBytes)) * MiBToBytes
			testFileName += ".with.shared.prefix"
			keyFuzzFunc = newGenerateFuzzWithSharedPrefixes()
		} else {
			curSize = (1 + r.Intn(DefaultMaxSizeKiB)) * KiBToBytes
			keyFuzzFunc = newGenerateFuzz()
		}

		keys := prepareSortedSlice(curSize, keyFuzzFunc)
		writerOptions := newDefaultWriterOptions()
		createTestInputFiles(keys, newGenerateFuzz(), curSize, testFileName, writerOptions)
	}
}

func writeTwoLevelIdxSst() {
	sizeBytes := DefaultTwoLevelSstSizeBytes
	generateNanoidKey := newGenerateNanoid(DefaultKeySizeBytes)
	keys := prepareSortedSlice(sizeBytes, generateNanoidKey)

	writerOptions := newDefaultWriterOptions()
	writerOptions.IndexBlockSize = 5 // setting the index block size target to a small number to make 2-level index get enabled
	generateNanoidValue := newGenerateNanoid(DefaultValueSizeBytes)
	createTestInputFiles(keys, generateNanoidValue, sizeBytes, "two.level.idx", writerOptions)
}

func prepareSortedSlice(size int, genKey func() (string, error)) []string {
	var keySumBytes int
	var slice []string
	// Keep generating keys until we reach the desired slice size. This guarantees that the sstable writer will have
	// enough data to write an sstable of the desired size.
	for keySumBytes < size {
		key, _ := genKey()
		if key == "" { // TODO(Tals): remove after resolving https://github.com/treeverse/lakeFS/issues/2419
			continue
		}
		keySumBytes += len(key)
		slice = append(slice, key)
	}
	sort.Strings(slice)
	return slice
}

// create sst and json files that will be used to unit test the sst block parser (clients/spark/core/src/main/scala/io/treeverse/jpebble/BlockParser.scala)
// The sst file is the test input, and the json file encapsulates the expected parser output.
func createTestInputFiles(keys []string, genValue func() (string, error), size int, name string,
	writerOptions sstable.WriterOptions) {
	fmt.Printf("Generate %s of size %d bytes\n", name, size)
	sstFileName := name + ".sst"
	sstFile, err := os.Create(sstFileName)
	if err != nil {
		panic(err)
	}
	writer := sstable.NewWriter(sstFile, writerOptions)
	defer func() {
		_ = writer.Close()
	}()

	expectedContents := make([]Entry, 0, len(keys))
	for _, k := range keys {
		if writer.EstimatedSize() >= uint64(size) {
			break
		}
		v, err := genValue()
		if err != nil {
			panic(err)
		}
		if err := writer.Set([]byte(k), []byte(v)); err != nil {
			panic(fmt.Errorf("setting key and value: %w, into %s", err, sstFileName))
		}
		expectedContents = append(expectedContents, Entry{Key: k, Value: v})
	}

	// save the expected contents as json
	jsonFileName := name + ".json"
	jsonWriter, err := os.Create(jsonFileName)
	if err != nil {
		panic(err)
	}
	defer func() {
		jsonWriter.Close()
	}()
	err = json.NewEncoder(jsonWriter).Encode(expectedContents)
	if err != nil {
		panic(err)
	}
}

// staticCollector - Copied from pkg/graveler/sstable/collectors.go. lakeFS populates the following user properties for each sstable
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

func getDefaultTablePropertyCollectors(m map[string]string) []func() sstable.TablePropertyCollector {
	return []func() sstable.TablePropertyCollector{NewStaticCollector(m)}
}
