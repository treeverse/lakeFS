package mocks

import (
	"fmt"
	"log"
	"os"

	"github.com/cockroachdb/pebble"
	table "github.com/cockroachdb/pebble/sstable"
	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/forest/sstable"
)

type Bwc struct {
	counter int
}

func (b *Bwc) CloseWriterAsync(_ sstable.Writer) error {
	b.counter++
	return nil
}

var Cache *pebble.Cache

var firstSSTactivation bool = true

func (b *Bwc) Wait() ([]sstable.WriteResult, error) {
	res := make([]sstable.WriteResult, 0)
	for i := 0; i < b.counter; i++ {
		t := fmt.Sprintf("%03d", i)
		r := sstable.WriteResult{
			Last:      rocks.Path(t),
			SSTableID: sstable.SSTableID(t),
		}
		res = append(res, r)
	}
	return res, nil
}

type SstMgr struct {
	NumToRead int
	Sstid     int
}

func (s *SstMgr) GetEntry(path rocks.Path, tid sstable.SSTableID) (*rocks.Entry, error) {
	panic("GetEntry not implemented")
	//return &rocks.Entry{},nil
}

func (s *SstMgr) NewSSTableIterator(tid sstable.SSTableID, from rocks.Path) (rocks.EntryIterator, error) {
	if firstSSTactivation {
		var cacheSize int64 = 1 << 31 // 2 GB cache size
		Cache = pebble.NewCache(cacheSize)
		firstSSTactivation = false
	}
	f, err := os.Open(string(tid) + ".json")
	if err != nil {
		return nil, err
	}
	r, err := table.NewReader(f, table.ReaderOptions{Cache: Cache})
	if err != nil {
		return nil, err
	}
	i, err := r.NewIter(nil, nil)
	return &DummyIter{f: f,
		r: r,
		i: i}, nil
}

func (s *SstMgr) GetWriter() (sstable.Writer, error) {
	s.Sstid++
	name := fmt.Sprintf("c-%05d", s.Sstid)
	f, err := os.Create(name + ".sst")
	if err != nil {
		panic(err)
	}
	w := table.NewWriter(f, table.WriterOptions{})
	return &DummyWriter{writer: w,
		filename: name,
		f:        f}, nil
}

type DummyIter struct {
	f      *os.File
	r      *table.Reader
	i      table.Iterator
	err    error
	closed bool
	value  rocks.EntryRecord
	Count  int
}

func (d *DummyIter) Next() bool {
	d.Count++
	if d.Count < d.MaxNum {
		return true
	}
	return false
}

func (d *DummyIter) SeekGE(id rocks.Path) bool {
	return true
}

func (d *DummyIter) Value() *rocks.EntryRecord {
	return &rocks.EntryRecord{}
}

func (d *DummyIter) Err() error {
	return nil
}

func (d *DummyIter) Close() {
	panic("implement me")
}

type DummyWriter struct {
	writer   *table.Writer
	lastKey  rocks.Path
	filename string
	f        *os.File
	RowNum   int
}

func (d *DummyWriter) WriteEntry(entry rocks.EntryRecord) error {
	if d.lastKey == entry.Path {
		return nil
	}
	if d.lastKey > entry.Path {
		log.Fatal("unsorted keys ", d.lastKey, " , ", entry.Path, "\n")
	}
	d.RowNum++
	d.lastKey = entry.Path
	t := string(entry.Path)
	l := len(t)
	i := l - 50
	if i < 0 {
		i = 0
	}
	value := []byte(t[i:])
	if err := d.writer.Set([]byte(t), value); err != nil {
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
	d.parts = append(d.parts, sstable.WriteResult{SSTableID: sstable.SSTableID(z.filename),
		Last: rocks.Path(z.lastKey),
	})
	fmt.Printf("file %s  number of lines: %d\n", z.filename, z.RowNum)
	return z.writer.Close()
}

func (d *DummyBatchCloser) Wait() ([]sstable.WriteResult, error) {
	return d.parts, nil
}
