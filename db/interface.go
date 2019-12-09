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

type Namespace []byte

type CompositeKey [][]byte

func (c CompositeKey) AsKey() []byte {
	var buf bytes.Buffer
	for i, part := range c {
		buf.Write(part)
		if i < len(c)-1 {
			buf.WriteByte(0x00)
		}
	}
	return buf.Bytes()
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

func (c CompositeKey) String() string {
	asString := make([]string, len(c))
	for i, p := range c {
		asString[i] = fmt.Sprintf("\"%s\"", string(p))
	}
	partsString := strings.Join(asString, ", ")
	return fmt.Sprintf("{%s}", partsString)
}

func KeyFromBytes(key []byte) CompositeKey {
	var buf bytes.Buffer
	ck := CompositeKey{}
	for _, b := range key {
		if b == 0x00 {
			ck = append(ck, buf.Bytes())
			buf = bytes.Buffer{}
		} else {
			buf.WriteByte(b)
		}
	}
	if buf.Len() > 0 {
		ck = append(ck, buf.Bytes())
	}
	return ck
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
	RangePrefixGreaterThan(space Namespace, prefix CompositeKey, greaterThan []byte) (Iterator, IteratorCloseFn)
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
