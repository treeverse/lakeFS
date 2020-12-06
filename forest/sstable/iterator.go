package sstable

import (
	"github.com/cockroachdb/pebble/sstable"
	"github.com/treeverse/lakefs/graveler"
)

// Iterator returns ordered iteration of the SSTable entries
type Iterator struct {
	it sstable.Iterator

	currKey   graveler.Key
	currValue *graveler.Value
	err       error
}

func (iter *Iterator) SeekGE(key graveler.Key) bool {
	return iter.handleKeyVal(iter.it.SeekGE([]byte(key)))
}

func (iter *Iterator) Next() bool {
	return iter.handleKeyVal(iter.it.Next())
}

func (iter *Iterator) handleKeyVal(key *sstable.InternalKey, val []byte) bool {
	if key == nil && val == nil {
		iter.currKey = nil
		iter.currValue = nil
		return false
	}

	iter.currKey = key.UserKey
	iter.currValue, iter.err = serialize.deserializeValue(val)

	if iter.err != nil {
		return false
	}

	return true
}

func (iter *Iterator) Value() *graveler.ValueRecord {
	if iter.currKey == nil {
		return nil
	}

	return &graveler.ValueRecord{
		Key:   iter.currKey,
		Value: iter.currValue,
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
