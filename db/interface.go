package db

import (
	"github.com/golang/protobuf/proto"
)

type KeyValue struct {
	Key   []byte
	Value []byte
}

type Namespace []byte

type CompositeKey [][]byte

func (c CompositeKey) AsKey() []byte {
	buf := make([]byte, 0)
	for i, part := range c {
		if i < len(c)-1 {
			buf = append(part, 0x00)
		} else {
			buf = append(part)
		}
	}
	return buf
}

func (c CompositeKey) With(parts ...[]byte) CompositeKey {
	n := append(CompositeKey{}, c...)
	n = append(n, parts...)
	return n
}

func CompositeStrings(parts ...string) CompositeKey {
	n := CompositeKey{}
	for _, k := range parts {
		n = append(n, []byte(k))
	}
	return n
}

type ProtoGenFn func() proto.Message

type Iterator interface {
	Advance() bool
	Get() (kv KeyValue, e error)
	Close()
}

type ReadQuery interface {
	Get(space Namespace, key CompositeKey) (KeyValue, error)
	GetAsProto(msg proto.Message, space Namespace, key CompositeKey) error
	Range(space Namespace) Iterator
	RangePrefix(space Namespace, prefix CompositeKey) Iterator
	RangePrefixGreaterThan(space Namespace, prefix CompositeKey, greaterThan []byte) Iterator
}

type Query interface {
	ReadQuery
	Set(data []byte, space Namespace, key CompositeKey) error
	SetProto(msg proto.Message, space Namespace, key CompositeKey) error
	ClearPrefix(space Namespace, key CompositeKey) error
	Delete(space Namespace, key CompositeKey) error
}

type Store interface {
	ReadTransact(func(q ReadQuery) (interface{}, error)) (interface{}, error)
	Transact(func(q Query) (interface{}, error)) (interface{}, error)
}
