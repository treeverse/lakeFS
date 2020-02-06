package db

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/golang/protobuf/proto"
)

type KeyValue struct {
	Key   []byte
	Value []byte
}

const TupleSeparator = '\x00' // use null byte as part separator

type FlatKey []byte
type Namespace []byte

type CompositeKey [][]byte

func (c CompositeKey) AsKey() FlatKey {
	return bytes.Join(c, []byte{TupleSeparator})

}

func (key FlatKey) Children() FlatKey {
	return append(key, TupleSeparator)
}

func (c CompositeKey) With(parts ...[]byte) CompositeKey {
	return append(c, parts...)
}

func CompositeStrings(parts ...string) CompositeKey {
	n := make(CompositeKey, len(parts))
	for i, k := range parts {
		n[i] = []byte(k)
	}
	return n
}

func (c CompositeKey) String() string {
	asString := make([]string, len(c))
	for i, p := range c {
		asString[i] = fmt.Sprintf("\"%s\"", string(p))
	}
	partsString := strings.Join(asString, ", ")
	return fmt.Sprintf("{%s}", partsString)
}

func KeyFromBytes(key []byte) CompositeKey {
	return bytes.Split(key, []byte{TupleSeparator})
}

type ProtoGenFn func() proto.Message

type Iterator interface {
	Advance() bool
	Get() (kv KeyValue, e error)
	Close()
}

type IteratorCloseFn func()

type ReadQuery interface {
	Get(space Namespace, key CompositeKey) (KeyValue, error)
	GetAsProto(msg proto.Message, space Namespace, key CompositeKey) error
	Range(space Namespace) (Iterator, IteratorCloseFn)
	RangePrefix(space Namespace, prefix CompositeKey) (Iterator, IteratorCloseFn)
	RangePrefixGreaterThan(space Namespace, prefix, greaterThan CompositeKey) (Iterator, IteratorCloseFn)
}

type Query interface {
	ReadQuery
	Set(data []byte, space Namespace, key CompositeKey) error
	SetProto(msg proto.Message, space Namespace, key CompositeKey) error
	ClearPrefix(space Namespace, key CompositeKey) error
	ClearChildren(space Namespace, key CompositeKey) error
	Delete(space Namespace, key CompositeKey) error
}

type Store interface {
	ReadTransact(func(q ReadQuery) (interface{}, error)) (interface{}, error)
	Transact(func(q Query) (interface{}, error)) (interface{}, error)
}
