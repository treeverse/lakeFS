package mocks

import (
	"fmt"
	"log"
	"os"

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
	return &DummyIter{MaxNum: s.NumToRead}, nil
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
	MaxNum int
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
}

func (d DummyWriter) WriteEntry(entry rocks.EntryRecord) error {
	if d.lastKey >= entry.Path {
		log.Fatal("unsorted keys ", d.lastKey, " , ", entry.Path, "\n")
	}
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

func (d DummyBatchCloser) CloseWriterAsync(w sstable.Writer) error {
	z := w.(DummyWriter)
	d.parts = append(d.parts, sstable.WriteResult{SSTableID: sstable.SSTableID(z.filename),
		Last: rocks.Path(z.lastKey),
	})
	return z.writer.Close()
}

func (d DummyBatchCloser) Wait() ([]sstable.WriteResult, error) {
	return d.parts, nil
}
