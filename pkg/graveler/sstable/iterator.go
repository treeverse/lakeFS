package sstable

import (
	"github.com/cockroachdb/pebble/sstable"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
)

// Iterator returns ordered iteration of the SSTable entries
type Iterator struct {
	it sstable.Iterator

	currKey   *sstable.InternalKey
	currValue []byte

	postSeek bool
	err      error
	derefer  func() error
}

func NewIterator(it sstable.Iterator, derefer func() error) *Iterator {
	iter := &Iterator{
		it:      it,
		derefer: derefer,
	}

	return iter
}

func (iter *Iterator) SeekGE(lookup committed.Key) {
	iter.currKey, iter.currValue = iter.it.SeekGE(lookup)
	iter.postSeek = true
}

func (iter *Iterator) Next() bool {
	if !iter.postSeek {
		iter.currKey, iter.currValue = iter.it.Next()
	}
	iter.postSeek = false

	if iter.currKey == nil && iter.currValue == nil {
		return false
	}

	return true
}

func (iter *Iterator) Value() *committed.Record {
	if iter.currKey == nil || iter.err != nil || iter.postSeek {
		return nil
	}

	return &committed.Record{
		Key:   iter.currKey.UserKey,
		Value: iter.currValue,
	}
}

func (iter *Iterator) Err() error {
	return iter.err
}

func (iter *Iterator) Close() {
	if iter.it == nil {
		return
	}
	err := iter.it.Close()
	iter.updateOnNilErr(err)

	err = iter.derefer()
	iter.updateOnNilErr(err)

	iter.it = nil
}

func (iter *Iterator) updateOnNilErr(err error) {
	if iter.err == nil {
		// avoid overriding earlier errors
		iter.err = err
	}
}
