package sstable

import (
	"github.com/cockroachdb/pebble/sstable"
	"github.com/treeverse/lakefs/graveler"
)

// Iterator returns ordered iteration of the SSTable entries
type Iterator struct {
	it  sstable.Iterator
	ser serializer

	currKey   *sstable.InternalKey
	currValue []byte

	valParsed *graveler.Value

	postSeek bool
	err      error
	derefer  func() error
}

func NewIterator(it sstable.Iterator, ser serializer, derefer func() error, from graveler.Key) *Iterator {
	iter := &Iterator{
		it:      it,
		ser:     ser,
		derefer: derefer,
	}

	if from != nil{
		iter.SeekGE(from)
	}

	return iter
}

func (iter *Iterator) SeekGE(lookup graveler.Key) {
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

	iter.valParsed, iter.err = iter.ser.DeserializeValue(iter.currValue)
	return iter.err == nil
}

func (iter *Iterator) Value() *graveler.ValueRecord {
	if iter.currKey == nil || iter.err != nil || iter.postSeek {
		return nil
	}

	return &graveler.ValueRecord{
		Key:   iter.currKey.UserKey,
		Value: iter.valParsed,
	}
}

func (iter *Iterator) Err() error {
	return iter.err
}

func (iter *Iterator) Close() {
	err := iter.it.Close()
	iter.updateOnNilErr(err)

	err = iter.derefer()
	iter.updateOnNilErr(err)
}

func (iter *Iterator) updateOnNilErr(err error) {
	if iter.err == nil {
		// avoid overriding earlier errors
		iter.err = err
	}
}
