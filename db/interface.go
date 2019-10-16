package db

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/golang/protobuf/proto"
)

type Iterator interface {
	Advance() bool
	Get() (kv fdb.KeyValue, e error)
	MustGet() fdb.KeyValue
}

type FutureValue interface {
	Get() ([]byte, error)
	MustGet() []byte
	Cancel()
}

type ReadQuery interface {
	Snapshot() ReadQuery
	Get(space subspace.Subspace, parts ...tuple.TupleElement) FutureValue
	GetAsProto(msg proto.Message, space subspace.Subspace, parts ...tuple.TupleElement) error
	RangePrefix(space subspace.Subspace, parts ...tuple.TupleElement) Iterator
}

type Query interface {
	ReadQuery
	Set(data []byte, space subspace.Subspace, parts ...tuple.TupleElement)
	SetProto(msg proto.Message, space subspace.Subspace, parts ...tuple.TupleElement) error
	ClearPrefix(space subspace.Subspace, parts ...tuple.TupleElement)
	Delete(space subspace.Subspace, parts ...tuple.TupleElement)
}

type Store interface {
	Space(name string) subspace.Subspace
	ReadTransact([]tuple.TupleElement, func(q ReadQuery) (interface{}, error)) (interface{}, error)
	Transact([]tuple.TupleElement, func(q Query) (interface{}, error)) (interface{}, error)
}
