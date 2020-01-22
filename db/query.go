package db

import (
	"bytes"
	"fmt"
	"strings"

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

	item      KeyValue
	itemError error
}

func (it *dbPrefixIterator) Advance() bool {
	if !it.iter.Valid() {
		return false
	}
	if len(it.prefix) > 0 && !it.iter.ValidForPrefix(it.prefix) {
		return false
	}
	item := it.iter.Item()
	// cache it
	it.item = KeyValue{}
	it.item.Key = item.KeyCopy(nil)
	it.item.Value, it.itemError = item.ValueCopy(nil)
	it.iter.Next()
	if strings.Contains(string(it.item.Key), "gz.3") {
		fmt.Printf("skip == '%v'\nkey ==  '%v'\n", it.skip, it.item.Key)
	}
	if len(it.skip) > 0 && bytes.Equal(it.item.Key, it.skip) {
		return it.Advance()
	}
	return true
}

func (it *dbPrefixIterator) Get() (KeyValue, error) {
	return it.item, it.itemError
}
func (it *dbPrefixIterator) Close() {
	it.iter.Close()
}

func (q *DBReadQuery) pack(ns Namespace, key CompositeKey) []byte {
	parts := make(CompositeKey, len(key)+1)
	parts[0] = part{data: ns}
	for i, k := range key {
		parts[i+1] = k
	}
	return parts.AsKey()
}

func (q *DBReadQuery) Get(space Namespace, key CompositeKey) (KeyValue, error) {
	kv := KeyValue{}
	item, err := q.tx.Get(q.pack(space, key))
	if err == badger.ErrKeyNotFound {
		return kv, ErrNotFound
	}
	if err != nil {
		return kv, err
	}
	kv.Key = item.KeyCopy(nil)
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

func (q *DBReadQuery) Range(space Namespace) (Iterator, IteratorCloseFn) {
	opts := badger.DefaultIteratorOptions
	it := q.tx.NewIterator(opts)
	pref := CompositeKey{part{data: space}}.AsKey()
	it.Seek(pref) // go to the correct offset
	return &dbPrefixIterator{
			prefix: pref,
			iter:   it,
		}, func() {
			it.Close()
		}
}

func (q *DBReadQuery) RangePrefix(space Namespace, prefix CompositeKey) (Iterator, IteratorCloseFn) {
	opts := badger.DefaultIteratorOptions
	pref := q.pack(space, prefix)
	it := q.tx.NewIterator(opts)
	it.Seek(pref) // go to the correct offset
	return &dbPrefixIterator{
			prefix: pref,
			iter:   it,
		}, func() {
			it.Close()
		}
}

func (q *DBReadQuery) RangePrefixGreaterThan(space Namespace, prefix CompositeKey, greaterThan []byte) (Iterator, IteratorCloseFn) {
	opts := badger.DefaultIteratorOptions
	pref := q.pack(space, prefix)
	opts.Prefix = pref
	it := q.tx.NewIterator(opts)
	offset := q.pack(space, prefix.WithGlob(greaterThan))
	it.Seek(offset) // go to the correct offset
	return &dbPrefixIterator{
			prefix: pref,
			skip:   offset,
			iter:   it,
		}, func() {
			it.Close()
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
	keys := make([][]byte, 0)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		k := it.Item().KeyCopy(nil)
		keys = append(keys, k)
	}
	it.Close()

	// do the actual deletion
	i := 0
	for _, k := range keys {
		err = q.tx.Delete(k)
		if err != nil {
			return err
		}
		i++
	}
	return nil
}

func (q *DBQuery) Delete(space Namespace, key CompositeKey) error {
	return q.tx.Delete(q.pack(space, key))
}
