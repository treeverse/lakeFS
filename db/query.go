package db

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type ReadQuery struct {
	Context []tuple.TupleElement
	tx      fdb.ReadTransaction
}

type Query struct {
	*ReadQuery
	tx fdb.Transaction
}

func (q *ReadQuery) pack(space subspace.Subspace, parts ...tuple.TupleElement) fdb.Key {
	ctxTuple := append(tuple.Tuple{}, q.Context...)
	parts = append(ctxTuple, parts...)
	return space.Pack(parts)
}

func (q *ReadQuery) get(space subspace.Subspace, parts ...tuple.TupleElement) fdb.FutureByteSlice {
	return q.tx.Get(q.pack(space, parts...))
}

func (q *ReadQuery) rangePrefix(space subspace.Subspace, parts ...tuple.TupleElement) *fdb.RangeIterator {
	begin := q.pack(space, parts...)
	return q.tx.GetRange(fdb.KeyRange{
		Begin: begin,
		End:   append(begin, 0xFF),
	}, fdb.RangeOptions{}).Iterator()
}

func (q *Query) set(data []byte, space subspace.Subspace, parts ...tuple.TupleElement) {
	q.tx.Set(q.pack(space, parts...), data)
}
func (q *Query) clearPrefix(space subspace.Subspace, parts ...tuple.TupleElement) {
	begin := q.pack(space, parts...)
	end := append(begin, 0xFF)
	q.tx.ClearRange(&fdb.KeyRange{
		Begin: begin,
		End:   end,
	})
}

func (q *Query) delete(space subspace.Subspace, parts ...tuple.TupleElement) {
	q.tx.Clear(q.pack(space, parts...))
}
