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

const TupleSeparator = 0x00 // use null byte as part separator

type Namespace []byte

type part struct {
	data []byte
	open bool
}

type CompositeKey []part

func (c CompositeKey) AsKey() []byte {
	var buf bytes.Buffer
	for _, part := range c {
		buf.Write(part.data)
		if !part.open {
			buf.WriteByte(TupleSeparator)
		}
	}
	return buf.Bytes()
}

func (c CompositeKey) With(parts ...[]byte) CompositeKey {
	cparts := make([]part, len(parts))
	for i, p := range parts {
		cparts[i] = part{data: p}
	}
	n := append(CompositeKey{}, c...)
	n = append(n, cparts...)
	return n
}

func (c CompositeKey) WithGlob(p []byte) CompositeKey {
	return append(c, part{data: p, open: true})
}

func CompositeStrings(parts ...string) CompositeKey {
	n := CompositeKey{}
	for _, k := range parts {
		n = append(n, part{data: []byte(k)})
	}
	return n
}

func (c CompositeKey) String() string {
	asString := make([]string, len(c))
	for i, p := range c {
		if p.open {
			asString[i] = fmt.Sprintf("\"%s*\"", string(p.data))
		} else {
			asString[i] = fmt.Sprintf("\"%s\"", string(p.data))
		}

	}
	partsString := strings.Join(asString, ", ")
	return fmt.Sprintf("{%s}", partsString)
}

func KeyFromBytes(key []byte) CompositeKey {
	var buf bytes.Buffer
	ck := CompositeKey{}
	for _, b := range key {
		if b == TupleSeparator {
			ck = append(ck, part{data: buf.Bytes(), open: false})
			buf = bytes.Buffer{}
		} else {
			buf.WriteByte(b)
		}
	}
	if buf.Len() > 0 {
		ck = append(ck, part{data: buf.Bytes(), open: true})
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
