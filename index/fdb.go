package index

import (
	"versio-index/model"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/golang/protobuf/proto"
	"golang.org/x/xerrors"
)

type DB struct {
	workspace     subspace.Subspace
	workspaceMeta subspace.Subspace
	tries         subspace.Subspace
	blobs         subspace.Subspace
	commits       subspace.Subspace
	branches      subspace.Subspace
	refCounts     subspace.Subspace
}

type Query struct {
	db   *DB
	repo *Repo
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
	parts = append(tuple.Tuple{q.repo.ClientID, q.repo.RepoID}, parts...)
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

func (r *QueryReader) ReadPath(branch, path string) (*Blob, error) {
	// read the blob's hash addr
	data := r.get(r.query.db.workspace, branch, path)
	if data == nil {
		return nil, ErrNotFound
	}
	addr := string(data.MustGet())

	// read blocks
	blocks := make([]string, 0)
	iterator := r.rangePrefix(r.query.db.blobs, addr)
	for iterator.Advance() {
		blocks = append(blocks, string(iterator.MustGet().Value))
	}

	// read the blob's metadata
	meta := make(map[string]string)
	iterator = r.rangePrefix(r.query.db.workspaceMeta, branch, path)
	for iterator.Advance() {
		kv := iterator.MustGet()
		key, err := r.query.db.workspaceMeta.Unpack(kv.Key)
		if err != nil {
			return nil, err
		}
		meta[key[5].(string)] = string(kv.Value)
	}

	// assemble result
	blob := &Blob{
		Address:  addr,
		Blocks:   blocks,
		Metadata: meta,
	}
	return blob, nil
}

func (r *QueryReader) ReadBranch(branch string) *Branch {
	// read branch attributes
	commitFuture := r.get(r.query.db.branches, branch, "commit_address")
	workspaceFuture := r.get(r.query.db.branches, branch, "workspace_root")
	return &Branch{
		Name:          branch,
		CommitAddress: string(commitFuture.MustGet()),
		WorkspaceRoot: string(workspaceFuture.MustGet()),
	}
}

func (q *ReadQuery) ReadBlob(addr string) (*model.Blob, error) {
	blobKey := q.db.blobs.Pack(tuple.Tuple{q.repo.ClientID, q.repo.RepoID, addr})
	blobData := q.tx.Get(blobKey).MustGet()
	if blobData == nil {
		return nil, ErrNotFound
	}
	blob := &model.Blob{}
	err := proto.Unmarshal(blobData, blob)
	if err != nil {
		return nil, xerrors.Errorf("unable to read file: %w", err)
	}
	return blob, nil
}

func (q *ReadQuery) GetEntry(parentAddr, name string) (*model.Entry, error) {
	key := q.db.trieEntries.Pack(tuple.Tuple{q.repo.ClientID, q.repo.RepoID, parentAddr, name})
	data := q.tx.Get(key).MustGet()
	if entryData == nil {
		return nil, ErrNotFound
	}
	ent := &model.Entry{}
	err := proto.Unmarshal(data, ent)
	if err != nil {
		return nil, xerrors.Errorf("unable to read path: %w", err)
	}
	return ent, nil
}

func (q *ReadQuery) ListEntries(addr string) ([]*model.Entry, error) {
	start := q.db.trieEntries.Pack(tuple.Tuple{q.repo.ClientID, q.repo.RepoID, addr})
	end := q.db.trieEntries.Pack(tuple.Tuple{q.repo.ClientID, q.repo.RepoID, addr, 0xFF})
	iter := q.tx.GetRange(fdb.KeyRange{Begin: start, End: end}, fdb.RangeOptions{}).Iterator()
	entries := make([]*model.Entry, 0)
	for iter.Advance() {
		next := iter.MustGet()
		data := next.Value
		entry := &model.Entry{}
		err := proto.Unmarshal(data, entry)
		if err != nil {
			return nil, xerrors.Errorf("unable to list path: %w", err)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (q *Query) WriteWorkspacePath(branch Branch, path string, blob *model.Blob) error {
	blobKey := q.db.workspace.Pack(tuple.Tuple{q.repo.ClientID, q.repo.RepoID, branch, path})
	blobData, err := proto.Marshal(blob)
	if err != nil {
		return ErrBadBlock
	}
	q.tx.Set(blobKey, blobData)
	return nil
}

func (q *Query) DeleteWorkspacePath(branch Branch, path string) error {
	blobKey := q.db.workspace.Pack(tuple.Tuple{q.repo.ClientID, q.repo.RepoID, branch, path})
	blob := &model.Blob{
		Metadata: map[string]string{"_tombstone": "true"},
	}
	blobData, err := proto.Marshal(blob)
	if err != nil {
		return ErrBadBlock
	}
	q.tx.Set(blobKey, blobData)
	return nil
}
