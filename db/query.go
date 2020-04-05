package db

import (
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
	return true
}

func (it *dbPrefixIterator) Get() (KeyValue, error) {
	return it.item, it.itemError
}

func (it *dbPrefixIterator) Close() {
	it.iter.Close()
}

func (q *DBReadQuery) pack(ns Namespace, key CompositeKey) FlatKey {
	parts := make(CompositeKey, len(key)+1)
	parts[0] = ns
	for i, k := range key {
		parts[i+1] = k
	}
	return parts.AsKey()
}

func (q *DBQuery) children(ns Namespace, key CompositeKey) FlatKey {
	return q.pack(ns, key).Children()
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

func (q *DBReadQuery) RangeAll() (Iterator, IteratorCloseFn) {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte("")
	it := q.tx.NewIterator(opts)
	it.Seek(opts.Prefix)
	return &dbPrefixIterator{
			iter:   it,
			prefix: []byte(""),
		}, func() {
			it.Close()
		}
}

func (q *DBReadQuery) Range(space Namespace) (Iterator, IteratorCloseFn) {
	opts := badger.DefaultIteratorOptions
	it := q.tx.NewIterator(opts)
	pref := CompositeKey{space}.AsKey()
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

func greaterThan(key CompositeKey) CompositeKey {
	return KeyFromBytes(key.AsKey().Children())
}

func (q *DBReadQuery) RangePrefixGreaterThan(space Namespace, prefix CompositeKey, gt CompositeKey) (Iterator, IteratorCloseFn) {
	opts := badger.DefaultIteratorOptions
	pref := q.pack(space, prefix)
	opts.Prefix = pref
	it := q.tx.NewIterator(opts)
	next := greaterThan(gt)
	offset := q.pack(space, next)
	it.Seek(offset) // go to the correct offset
	return &dbPrefixIterator{
			prefix: pref,
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

func (q *DBQuery) clearKeyPrefix(prefix FlatKey) error {
	opts := badger.DefaultIteratorOptions
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
func (q *DBQuery) ClearPrefix(space Namespace, key CompositeKey) error {
	prefix := q.pack(space, key) // go to the correct offset
	return q.clearKeyPrefix(prefix)
}

func (q *DBQuery) ClearChildren(space Namespace, key CompositeKey) error {
	prefix := q.children(space, key)
	return q.clearKeyPrefix(prefix)
}

func (q *DBQuery) Delete(space Namespace, key CompositeKey) error {
	return q.tx.Delete(q.pack(space, key))
}
