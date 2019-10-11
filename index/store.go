package index

import (
	"versio-index/index/model"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"

	"github.com/golang/protobuf/proto"
)

type ReadOnlyTransaction interface {
	ListWorkspace(branch string) ([]*model.WorkspaceEntry, error)
	ReadFromWorkspace(branch, path string) (*model.WorkspaceEntry, error)
	ReadBranch(branch string) (*model.Branch, error)
	ReadBlob(addr string) (*model.Blob, error)
	ReadCommit(addr string) (*model.Commit, error)
	ListEntries(addr string) ([]*model.Entry, error)
	ReadEntry(treeAddress, entryType, name string) (*model.Entry, error)
}

type Transaction interface {
	ReadOnlyTransaction
	WriteToWorkspacePath(branch, path string, entry *model.WorkspaceEntry) error
	ClearWorkspace(branch string)
	WriteEntry(treeAddress, entryType, name string, entry *model.Entry) error
	WriteBlob(addr string, blob *model.Blob) error
	WriteCommit(addr string, commit *model.Commit) error
	WriteBranch(name string, branch *model.Branch) error
	DeleteBranch(name string)
}

type Store interface {
	ReadTransact(*model.Repo, func(transaction ReadOnlyTransaction) (interface{}, error)) (interface{}, error)
	Transact(*model.Repo, func(Transaction) (interface{}, error)) (interface{}, error)
}

type fdbStore struct {
	db        fdb.Database
	workspace subspace.Subspace
	trees     subspace.Subspace
	entries   subspace.Subspace
	blobs     subspace.Subspace
	commits   subspace.Subspace
	branches  subspace.Subspace
	refCounts subspace.Subspace
}

func (s *fdbStore) ReadTransact(repo *model.Repo, fn func(transaction ReadOnlyTransaction) (interface{}, error)) (interface{}, error) {
	return s.db.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		t := &fdbReadOnlyTransaction{
			workspace: s.workspace,
			trees:     s.trees,
			entries:   s.entries,
			blobs:     s.blobs,
			commits:   s.commits,
			branches:  s.branches,
			refCounts: s.refCounts,
			query: readQuery{
				repo: repo,
				tx:   tx,
			},
		}
		return fn(t)
	})
}

func (s *fdbStore) Transact(repo *model.Repo, fn func(Transaction) (interface{}, error)) (interface{}, error) {
	return s.db.Transact(func(tx fdb.Transaction) (interface{}, error) {
		t := &fdbTransaction{
			query: query{
				readQuery: &readQuery{
					repo: repo,
					tx:   tx,
				},
				tx: tx,
			},
			fdbReadOnlyTransaction: fdbReadOnlyTransaction{
				workspace: s.workspace,
				trees:     s.trees,
				entries:   s.entries,
				blobs:     s.blobs,
				commits:   s.commits,
				branches:  s.branches,
				refCounts: s.refCounts,
				query: readQuery{
					repo: repo,
					tx:   tx,
				},
			},
		}
		return fn(t)
	})
}

type fdbReadOnlyTransaction struct {
	workspace subspace.Subspace
	trees     subspace.Subspace
	entries   subspace.Subspace
	blobs     subspace.Subspace
	commits   subspace.Subspace
	branches  subspace.Subspace
	refCounts subspace.Subspace

	query readQuery
}

type fdbTransaction struct {
	fdbReadOnlyTransaction
	query query
}

func (s *fdbReadOnlyTransaction) ListWorkspace(branch string) ([]*model.WorkspaceEntry, error) {
	iter := s.query.rangePrefix(s.workspace, branch)
	ws := make([]*model.WorkspaceEntry, 0)
	for iter.Advance() {
		kv := iter.MustGet()
		ent := &model.WorkspaceEntry{}
		err := proto.Unmarshal(kv.Value, ent)
		if err != nil {
			return nil, ErrIndexMalformed
		}
		ws = append(ws, ent)
	}
	return ws, nil
}

func (s *fdbReadOnlyTransaction) ReadFromWorkspace(branch, path string) (*model.WorkspaceEntry, error) {
	// read the blob's hash addr
	data := s.query.get(s.workspace, branch, path).MustGet()
	if data == nil {
		return nil, ErrNotFound
	}
	ent := &model.WorkspaceEntry{}
	err := proto.Unmarshal(data, ent)
	if err != nil {
		return nil, ErrIndexMalformed
	}
	return ent, nil
}

func (s *fdbReadOnlyTransaction) ReadBranch(branch string) (*model.Branch, error) {
	// read branch attributes
	data := s.query.get(s.branches, branch).MustGet()
	if data == nil {
		return nil, ErrNotFound
	}
	branchModel := &model.Branch{}
	err := proto.Unmarshal(data, branchModel)
	if err != nil {
		return nil, ErrIndexMalformed
	}
	return branchModel, nil
}

func (s *fdbReadOnlyTransaction) ReadBlob(addr string) (*model.Blob, error) {
	blobData := s.query.get(s.blobs, addr).MustGet()
	if blobData == nil {
		return nil, ErrNotFound
	}
	blob := &model.Blob{}
	err := proto.Unmarshal(blobData, blob)
	if err != nil {
		return nil, ErrIndexMalformed
	}
	return blob, nil
}

func (s *fdbReadOnlyTransaction) ReadCommit(addr string) (*model.Commit, error) {
	data := s.query.get(s.commits, addr).MustGet()
	if data == nil {
		return nil, ErrNotFound
	}
	commit := &model.Commit{}
	err := proto.Unmarshal(data, commit)
	if err != nil {
		return nil, ErrIndexMalformed
	}
	return commit, nil
}

func (s *fdbReadOnlyTransaction) ListEntries(addr string) ([]*model.Entry, error) {
	iter := s.query.rangePrefix(s.entries, addr)
	entries := make([]*model.Entry, 0)
	for iter.Advance() {
		entryData := iter.MustGet()
		entry := &model.Entry{}
		err := proto.Unmarshal(entryData.Value, entry)
		if err != nil {
			return nil, ErrIndexMalformed
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (s *fdbReadOnlyTransaction) ReadEntry(treeAddress, entryType, name string) (*model.Entry, error) {
	data := s.query.get(s.entries, treeAddress, entryType, name).MustGet()
	if data == nil {
		return nil, ErrNotFound
	}
	entry := &model.Entry{}
	err := proto.Unmarshal(data, entry)
	if err != nil {
		return nil, ErrIndexMalformed
	}
	return entry, nil
}

func (s *fdbTransaction) WriteToWorkspacePath(branch, path string, entry *model.WorkspaceEntry) error {
	data, err := proto.Marshal(entry)
	if err != nil {
		return ErrIndexMalformed
	}
	s.query.set(data, s.workspace, path)
	return nil
}

func (s *fdbTransaction) ClearWorkspace(branch string) {
	s.query.clearPrefix(s.workspace, s.query.repo.GetClientId(), s.query.repo.GetRepoId(), branch)
}

func (s *fdbTransaction) WriteEntry(treeAddress, entryType, name string, entry *model.Entry) error {
	data, err := proto.Marshal(entry)
	if err != nil {
		return err
	}
	s.query.set(data, s.entries, treeAddress, entryType, name)
	return nil
}

func (s *fdbTransaction) WriteBlob(addr string, blob *model.Blob) error {
	data, err := proto.Marshal(blob)
	if err != nil {
		return err
	}
	s.query.set(data, s.blobs, addr)
	return nil
}

func (s *fdbTransaction) WriteCommit(addr string, commit *model.Commit) error {
	data, err := proto.Marshal(commit)
	if err != nil {
		return err
	}
	s.query.set(data, s.commits, addr)
	return nil
}

func (s *fdbTransaction) WriteBranch(name string, branch *model.Branch) error {
	data, err := proto.Marshal(branch)
	if err != nil {
		return err
	}
	s.query.set(data, s.branches, name)
	return nil
}

func (s *fdbTransaction) DeleteBranch(name string) {
	s.query.delete(s.branches, name)
}
