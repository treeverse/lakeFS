package testutils

import "github.com/treeverse/lakefs/catalog"

type FakeEntryIterator struct {
	Entries []*catalog.EntryRecord
	Error   error
	index   int
}

func NewFakeEntryIterator(entries []*catalog.EntryRecord) *FakeEntryIterator {
	return &FakeEntryIterator{
		Entries: entries,
		index:   -1,
	}
}

func (f *FakeEntryIterator) Next() bool {
	if f.Error != nil {
		return false
	}
	f.index++
	return f.index < len(f.Entries)
}

func (f *FakeEntryIterator) SeekGE(id catalog.Path) {
	if f.Error != nil {
		return
	}
	for i, ent := range f.Entries {
		if ent.Path >= id {
			f.index = i - 1
			return
		}
	}
}

func (f *FakeEntryIterator) Value() *catalog.EntryRecord {
	if f.Error != nil || f.index < 0 || f.index >= len(f.Entries) {
		return nil
	}
	return f.Entries[f.index]
}

func (f *FakeEntryIterator) Err() error {
	return f.Error
}

func (f *FakeEntryIterator) Close() {}
