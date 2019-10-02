package indexer

import (
	"versio-index/model"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/golang/protobuf/proto"
	"golang.org/x/xerrors"
)

type DB struct {
	database    fdb.Database
	workspace   subspace.Subspace
	tries       subspace.Subspace
	trieEntries subspace.Subspace
	blobs       subspace.Subspace
	commits     subspace.Subspace
	branches    subspace.Subspace
	refCounts   subspace.Subspace
}

type Query struct {
	db   *DB
	repo *Repo

	tx fdb.Transaction
}

type ReadQuery struct {
	db   *DB
	repo *Repo

	tx fdb.ReadTransaction
}

func (q *ReadQuery) ReadWorkspacePath(branch Branch, path string) (*model.Blob, error) {
	blobKey := w.workspace.Pack(tuple.Tuple{q.repo.ClientID, q.repo.RepoID, branch, path})
	data := q.tx.Get(blobKey).MustGet()
	if data == nil {
		return nil, ErrNotFound
	}
	blob := &model.Blob{}
	err := proto.Unmarshal(data, blob)
	if err != nil {
		return nil, xerrors.Errorf("unable to read path data: %w", ErrIndexMalformed)
	}
	return blob, nil
}

func (q *ReadQuery) ReadBranch(branch Branch) (*model.Branch, error) {
	branchBytes := q.tx.Get(q.db.branches.Pack(tuple.Tuple{q.repo.ClientID, q.repo.RepoID, branch})).MustGet()
	if branchBytes == nil {
		// branch not found we fall back to master..
		branchBytes := q.tx.Get(q.db.branches.Pack(tuple.Tuple{q.repo.ClientID, q.repo.RepoID, DefaultBranch})).MustGet()
		if branchBytes == nil {
			return nil, ErrNotFound
		}
	}
	// parse branch
	branchData := &model.Branch{}
	err := proto.Unmarshal(branchBytes, branchData)
	if err != nil {
		return nil, xerrors.Errorf("unable to read branch data: %w", ErrIndexMalformed)
	}
	return branchData, nil
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
