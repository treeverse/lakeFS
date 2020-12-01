package sstable

import (
	"github.com/cockroachdb/pebble/sstable"
	"github.com/treeverse/lakefs/catalog/rocks"
)

// Iterator returns ordered iteration of the SSTable entries
type Iterator struct {
	it sstable.Iterator

	currPath  *rocks.Path
	currEntry *rocks.Entry
	err       error
}

func (iter *Iterator) SeekGE(path rocks.Path) bool {
	return iter.handleKeyVal(iter.it.SeekGE([]byte(path)))
}

func (iter *Iterator) Next() bool {
	return iter.handleKeyVal(iter.it.Next())
}

func (iter *Iterator) handleKeyVal(key *sstable.InternalKey, val []byte) bool {
	if key == nil && val == nil {
		iter.currPath = nil
		iter.currEntry = nil
		return false
	}

	path := rocks.Path(key.UserKey)
	iter.currPath = &path
	iter.currEntry, iter.err = deserializeEntry(val)

	if iter.err != nil {
		return false
	}

	return true
}

func (iter *Iterator) Value() *rocks.EntryRecord {
	if iter.currPath == nil {
		return nil
	}

	return &rocks.EntryRecord{
		Path:  *iter.currPath,
		Entry: iter.currEntry,
	}
}

func (iter *Iterator) Err() error {
	return iter.err
}

func (iter *Iterator) Close() {
	err := iter.it.Close()
	if iter.err == nil {
		// avoid overriding earlier errors
		iter.err = err
	}
}
