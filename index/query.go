package index

import (
	"versio-index/index/model"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type Query struct {
	workspace subspace.Subspace // store WorkspaceEntry objects per branch
	trees     subspace.Subspace // stores tree metadata objects
	entries   subspace.Subspace // enumerates tree entries (blobs and other trees, directories first)
	blobs     subspace.Subspace // stores simple blobs
	commits   subspace.Subspace // store commit objects
	branches  subspace.Subspace // store branch pointer and metadata
	refCounts subspace.Subspace // trie reference counts

	repo *model.Repo
}

type QueryReader struct {
	query *Query
	tx    fdb.ReadTransaction
}

type QueryWriter struct {
	query *Query
	tx    fdb.Transaction
}

func (q *Query) Reader(tx fdb.ReadTransaction) *QueryReader {
	return &QueryReader{query: q, tx: tx}
}

func (q *Query) Writer(tx fdb.Transaction) *QueryWriter {
	return &QueryWriter{query: q, tx: tx}
}

func (q *Query) pack(space subspace.Subspace, parts ...tuple.TupleElement) fdb.Key {
	parts = append(tuple.Tuple{q.repo.GetClientId(), q.repo.GetRepoId()}, parts...)
	return space.Pack(parts)
}

func (r *QueryReader) get(space subspace.Subspace, parts ...tuple.TupleElement) fdb.FutureByteSlice {
	return r.tx.Get(r.query.pack(space, parts...))
}

func (r *QueryReader) rangePrefix(space subspace.Subspace, parts ...tuple.TupleElement) *fdb.RangeIterator {
	begin := r.query.pack(space, parts...)
	return r.tx.GetRange(fdb.KeyRange{
		Begin: begin,
		End:   append(begin, 0xFF),
	}, fdb.RangeOptions{}).Iterator()
}

func (w *QueryWriter) set(data []byte, space subspace.Subspace, parts ...tuple.TupleElement) {
	w.tx.Set(w.query.pack(space, parts...), data)
}
func (w *QueryWriter) clearPrefix(space subspace.Subspace, parts ...tuple.TupleElement) {
	begin := w.query.pack(space, parts...)
	end := append(begin, 0xFF)
	w.tx.ClearRange(&fdb.KeyRange{
		Begin: begin,
		End:   end,
	})
}

func (w *QueryWriter) delete(space subspace.Subspace, parts ...tuple.TupleElement) {
	w.tx.Clear(w.query.pack(space, parts...))
}
