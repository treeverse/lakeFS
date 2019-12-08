package db

import (
	"bytes"

	"github.com/dgraph-io/badger"
	"github.com/golang/protobuf/proto"
)

type DBReadQuery struct {
	tx *badger.Txn
}

type DBQuery struct {
	*DBReadQuery
}

type dbPrefixIterator struct {
	skip   []byte
	prefix []byte
	iter   *badger.Iterator
}

func (it *dbPrefixIterator) Advance() bool {
	it.iter.Next()
	underPrefix := it.iter.ValidForPrefix(it.prefix)
	if !underPrefix {
		return false // no longer ranging the requested prefix
	}
	if it.skip != nil && bytes.Equal(it.iter.Item().Key(), it.skip) {
		it.iter.Next() // skip this requested key
		return it.iter.ValidForPrefix(it.prefix)
	}
	return true

}
func (it *dbPrefixIterator) Get() (KeyValue, error) {
	var err error
	kv := KeyValue{}
	item := it.iter.Item()
	kv.Key = item.Key()
	kv.Value, err = item.ValueCopy(nil)
	return kv, err
}
func (it *dbPrefixIterator) Close() {
	it.iter.Close()
}

func (q *DBReadQuery) pack(ns Namespace, key CompositeKey) []byte {
	parts := CompositeKey{ns}
	parts = parts.With(key...)
	return parts.AsKey()
}

func (q *DBReadQuery) Get(space Namespace, key CompositeKey) (KeyValue, error) {
	kv := KeyValue{}
	item, err := q.tx.Get(q.pack(space, key))
	if err != nil {
		return kv, err
	}
	kv.Key = item.Key()
	kv.Value, err = item.ValueCopy(nil)
	return kv, err

}

func (q *DBReadQuery) GetAsProto(msg proto.Message, space Namespace, key CompositeKey) error {
	data, err := q.Get(space, key)
	if err != nil {
		return err
	}
	err = proto.Unmarshal(data.Value, msg)
	if err != nil {
		return ErrSerialization
	}
	return nil
}

func (q *DBReadQuery) Range(space Namespace) Iterator {
	opts := badger.DefaultIteratorOptions
	it := q.tx.NewIterator(opts)
	return &dbPrefixIterator{iter: it}
}

func (q *DBReadQuery) RangePrefix(space Namespace, prefix CompositeKey) Iterator {
	opts := badger.DefaultIteratorOptions
	it := q.tx.NewIterator(opts)
	it.Seek(q.pack(space, prefix)) // go to the correct offset
	return &dbPrefixIterator{
		prefix: prefix.AsKey(),
		iter:   it,
	}
}

func (q *DBReadQuery) RangePrefixGreaterThan(space Namespace, prefix CompositeKey, greaterThan []byte) Iterator {
	opts := badger.DefaultIteratorOptions
	it := q.tx.NewIterator(opts)
	offset := prefix.With(greaterThan)
	it.Seek(q.pack(space, offset)) // go to the correct offset
	return &dbPrefixIterator{
		prefix: prefix.AsKey(),
		skip:   offset.AsKey(),
		iter:   it,
	}
}

func (q *DBQuery) Set(data []byte, space Namespace, key CompositeKey) error {
	return q.tx.Set(q.pack(space, key), data)
}

func (q *DBQuery) SetProto(msg proto.Message, space Namespace, key CompositeKey) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return ErrSerialization
	}
	return q.Set(data, space, key)
}

func (q *DBQuery) ClearPrefix(space Namespace, key CompositeKey) error {
	opts := badger.DefaultIteratorOptions
	prefix := q.pack(space, key) // go to the correct offset
	it := q.tx.NewIterator(opts)
	var err error
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		err = q.tx.Delete(it.Item().Key())
		if err != nil {
			return err
		}
	}
	return err
}

func (q *DBQuery) Delete(space Namespace, key CompositeKey) error {
	return q.tx.Delete(q.pack(space, key))
}
