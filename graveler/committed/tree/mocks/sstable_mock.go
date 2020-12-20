package mocks

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/cockroachdb/pebble"
	table "github.com/cockroachdb/pebble/sstable"
	gr "github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed/sstable"
)

type Bwc struct {
	counter int
}

func (b *Bwc) CloseWriterAsync(_ sstable.Writer) error {
	b.counter++
	return nil
}

var Cache *pebble.Cache
var ErrDuplicateKey = errors.New(" Can not write same key twice to SST")
var firstSSTActivation = true

func (b *Bwc) Wait() ([]sstable.WriteResult, error) {
	res := make([]sstable.WriteResult, 0)
	for i := 0; i < b.counter; i++ {
		t := fmt.Sprintf("%03d", i)
		r := sstable.WriteResult{
			Last:      gr.Key(t),
			SSTableID: sstable.ID(t),
		}
		res = append(res, r)
	}
	return res, nil
}

type SstMgr struct {
	Sstid int
}

func (s *SstMgr) GetEntry(key gr.Key, tid sstable.ID) (*gr.Value, error) {
	_ = key
	_ = tid
	panic("GetEntry not implemented")
}
func (s *SstMgr) GetValue(key gr.Key, tid sstable.ID) (*gr.Value, error) {
	_ = key
	_ = tid
	panic("GetValue not implemented")
}

var readerCache map[sstable.ID]*table.Reader

func (s *SstMgr) NewSSTableIterator(tid sstable.ID, from gr.Key) (gr.ValueIterator, error) {
	var err error
	var file *os.File
	if firstSSTActivation {
		var cacheSize int64 = 1 << 31 // 2 GB cache size
		Cache = pebble.NewCache(cacheSize)
		readerCache = make(map[sstable.ID]*table.Reader, 100)
		firstSSTActivation = false
	}
	reader, found := readerCache[tid]
	if !found {
		file, err = os.Open("testdata/" + string(tid) + ".sst")
		if err != nil {
			return nil, err
		}
		reader, err = table.NewReader(file, table.ReaderOptions{Cache: Cache})
		if err != nil {
			return nil, err
		}
		readerCache[tid] = reader
		fmt.Printf("reader cache size %d\n", len(readerCache))
	}

	i, err := reader.NewIter(nil, nil)
	if err != nil {
		return nil, err
	}
	d := DummyIter{f: file,
		r:        reader,
		iterator: i}
	var key *pebble.InternalKey
	var val []byte
	if len(from) == 0 {
		key, val = i.First()
	} else {
		key, val = i.SeekGE(from)
	}
	d.ValueFromPrevOperation = true
	d.handleResults(key, val)
	return &d, nil
}

type DummyIter struct {
	f                      *os.File
	r                      *table.Reader
	iterator               table.Iterator
	err                    error
	closed                 bool
	value                  *gr.ValueRecord
	Count                  int
	ValueFromPrevOperation bool
}

func (d *DummyIter) Next() bool {
	var key *pebble.InternalKey
	var val []byte
	if d.ValueFromPrevOperation {
		d.ValueFromPrevOperation = false
	} else {
		key, val = d.iterator.Next()
		d.handleResults(key, val)
	}
	if d.value != nil {
		return true
	} else {
		return false
	}
}
func (d *DummyIter) handleResults(key *pebble.InternalKey, val []byte) {
	if key != nil && d.iterator.Error() == nil {
		d.value = &gr.ValueRecord{
			Key: gr.Key(key.UserKey),
			Value: &gr.Value{
				Data: val},
		}
		d.err = nil
	} else {
		d.err = d.iterator.Error()
		d.value = nil
	}
}

func makeValue(key *pebble.InternalKey, value []byte) *gr.ValueRecord {
	v := &gr.ValueRecord{
		Key: gr.Key(key.UserKey),
		Value: &gr.Value{
			Data: value},
	}
	return v
}
func (d *DummyIter) SeekGE(id gr.Key) {
	d.ValueFromPrevOperation = true
	key, val := d.iterator.SeekGE(id)
	if key != nil && d.iterator.Error() == nil {
		d.value = makeValue(key, val)
		d.err = nil
	} else {
		d.err = d.iterator.Error()
		d.value = nil
	}
}

func (d *DummyIter) Value() *gr.ValueRecord {
	return d.value
}

func (d *DummyIter) Err() error {
	return d.err
}

func (d *DummyIter) Close() {
	d.iterator.Close()
}

type DummyWriter struct {
	writer   *table.Writer
	lastKey  gr.Key
	partName string
	f        *os.File
	RowNum   int
}

func (s *SstMgr) GetWriter() (sstable.Writer, error) {
	s.Sstid++
	partName := fmt.Sprintf("c-%05d", s.Sstid)
	name := "testdata/" + partName + ".sst"
	f, err := os.Create(name)
	if err != nil {
		panic(err)
	}
	w := table.NewWriter(f, table.WriterOptions{})
	return &DummyWriter{writer: w,
		partName: partName,
		f:        f,
	}, nil
}

func (d *DummyWriter) WriteRecord(valueRecord gr.ValueRecord) error {
	if bytes.Equal(d.lastKey, valueRecord.Key) {
		return ErrDuplicateKey
	}
	if bytes.Compare(d.lastKey, valueRecord.Key) > 0 {
		log.Fatal("unsorted keys ", d.lastKey, " , ", valueRecord.Key, "\n")
	}
	d.RowNum++
	d.lastKey = valueRecord.Key
	if err := d.writer.Set(valueRecord.Key, valueRecord.Data); err != nil {
		log.Fatal(err)
	}
	return nil
}

func (d DummyWriter) Close() (*sstable.WriteResult, error) {
	return nil, d.writer.Close()
}

type DummyBatchCloser struct {
	parts []sstable.WriteResult
}

func NewBatchCloser() *DummyBatchCloser {
	return &DummyBatchCloser{parts: make([]sstable.WriteResult, 0, 10)}
}

func (d *DummyBatchCloser) CloseWriterAsync(w sstable.Writer) error {
	z := w.(*DummyWriter)
	// todo: remove extension
	d.parts = append(d.parts, sstable.WriteResult{SSTableID: sstable.ID(z.partName),
		Last: z.lastKey,
	})
	fmt.Printf("file %s  number of lines: %d\n", z.partName, z.RowNum)
	return z.writer.Close() // do not close to enable caching
}

func (d *DummyBatchCloser) Wait() ([]sstable.WriteResult, error) {
	return d.parts, nil
}
