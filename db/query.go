package db

import (
	"versio-index/index/errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/golang/protobuf/proto"
)

type FDBReadQuery struct {
	Context []tuple.TupleElement
	tx      fdb.ReadTransaction
}

type FDBQuery struct {
	*FDBReadQuery
	tx fdb.Transaction
}

func (q *FDBReadQuery) Snapshot() ReadQuery {
	return &FDBReadQuery{
		Context: q.Context,
		tx:      q.tx.Snapshot(),
	}
}

func (q *FDBReadQuery) pack(space subspace.Subspace, parts ...tuple.TupleElement) fdb.Key {
	ctxTuple := append(tuple.Tuple{}, q.Context...)
	parts = append(ctxTuple, parts...)
	return space.Pack(parts)
}

func (q *FDBReadQuery) Get(space subspace.Subspace, parts ...tuple.TupleElement) FutureValue {
	return q.tx.Get(q.pack(space, parts...))
}

func (q *FDBReadQuery) GetAsProto(msg proto.Message, space subspace.Subspace, parts ...tuple.TupleElement) error {
	data := q.Get(space, parts...).MustGet()
	if data == nil {
		return ErrNotFound
	}
	err := proto.Unmarshal(data, msg)
	if err != nil {
		return errors.ErrIndexMalformed
	}
	return nil
}

type fdbFutureProtoValue struct {
	f  FutureValue
	fn ProtoGenFn
}

func (v *fdbFutureProtoValue) Get() (proto.Message, error) {
	data, err := v.f.Get()
	if data == nil {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	msg := v.fn()
	err = proto.Unmarshal(data, msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}
func (v *fdbFutureProtoValue) Cancel() {
	v.f.Cancel()
}

func (q *FDBReadQuery) FutureProto(generator ProtoGenFn, space subspace.Subspace, parts ...tuple.TupleElement) FutureProtoValue {
	return &fdbFutureProtoValue{
		f:  q.Get(space, parts...),
		fn: generator,
	}
}

func (q *FDBReadQuery) RangePrefix(space subspace.Subspace, parts ...tuple.TupleElement) Iterator {
	begin := q.pack(space, parts...)
	return q.tx.GetRange(fdb.KeyRange{
		Begin: begin,
		End:   append(begin, 0xFF),
	}, fdb.RangeOptions{}).Iterator()
}

func (q *FDBQuery) Set(data []byte, space subspace.Subspace, parts ...tuple.TupleElement) {
	q.tx.Set(q.pack(space, parts...), data)
}

func (q *FDBQuery) SetProto(msg proto.Message, space subspace.Subspace, parts ...tuple.TupleElement) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return errors.ErrIndexMalformed
	}
	q.Set(data, space, parts...)
	return nil
}

func (q *FDBQuery) ClearPrefix(space subspace.Subspace, parts ...tuple.TupleElement) {
	begin := q.pack(space, parts...)
	end := append(begin, 0xFF)
	q.tx.ClearRange(&fdb.KeyRange{
		Begin: begin,
		End:   end,
	})
}

func (q *FDBQuery) Delete(space subspace.Subspace, parts ...tuple.TupleElement) {
	q.tx.Clear(q.pack(space, parts...))
}
