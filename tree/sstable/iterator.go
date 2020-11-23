package sstable

import (
	"github.com/cockroachdb/pebble/sstable"
	"github.com/treeverse/lakefs/rocks3"
)

// Iterator returns ordered iteration of the SSTable entries
type Iterator struct {
	it sstable.Iterator

	currPath  *rocks3.Path
	currEntry *rocks3.Entry
	err       error
}

func (iter *Iterator) SeekGE(path rocks3.Path) bool {
	return iter.handleKeyVal(iter.it.SeekGE([]byte(path)))
}

func (iter *Iterator) Next() bool {
	return iter.handleKeyVal(iter.it.Next())
}

func (iter *Iterator) handleKeyVal(key *sstable.InternalKey, val []byte) bool {
	if key == nil && val == nil {
		return false
	}

	path := rocks3.Path(key.UserKey)
	iter.currPath = &path
	iter.currEntry, iter.err = deserializeEntry(val)

	if iter.err != nil {
		return false
	}

	return true
}

func (iter *Iterator) Value() (*rocks3.Path, *rocks3.Entry) {
	return iter.currPath, iter.currEntry
}

func (iter *Iterator) Error() error {
	return iter.err
}

func (iter *Iterator) Close() error {
	return iter.it.Close()
}
