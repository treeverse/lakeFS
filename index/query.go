package index

import (
	"versio-index/index/model"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type readQuery struct {
	repo *model.Repo
	tx   fdb.ReadTransaction
}

type query struct {
	*readQuery
	tx fdb.Transaction
}

func (q *readQuery) pack(space subspace.Subspace, parts ...tuple.TupleElement) fdb.Key {
	parts = append(tuple.Tuple{q.repo.GetClientId(), q.repo.GetRepoId()}, parts...)
	return space.Pack(parts)
}

func (q *readQuery) get(space subspace.Subspace, parts ...tuple.TupleElement) fdb.FutureByteSlice {
	return q.tx.Get(q.pack(space, parts...))
}

func (q *readQuery) rangePrefix(space subspace.Subspace, parts ...tuple.TupleElement) *fdb.RangeIterator {
	begin := q.pack(space, parts...)
	return q.tx.GetRange(fdb.KeyRange{
		Begin: begin,
		End:   append(begin, 0xFF),
	}, fdb.RangeOptions{}).Iterator()
}

func (q *query) set(data []byte, space subspace.Subspace, parts ...tuple.TupleElement) {
	q.tx.Set(q.pack(space, parts...), data)
}
func (q *query) clearPrefix(space subspace.Subspace, parts ...tuple.TupleElement) {
	begin := q.pack(space, parts...)
	end := append(begin, 0xFF)
	q.tx.ClearRange(&fdb.KeyRange{
		Begin: begin,
		End:   end,
	})
}

func (q *query) delete(space subspace.Subspace, parts ...tuple.TupleElement) {
	q.tx.Clear(q.pack(space, parts...))
}
