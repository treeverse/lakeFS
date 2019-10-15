package store

import (
	"versio-index/index/errors"
	"versio-index/index/model"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"

	"github.com/golang/protobuf/proto"
)

type ReadOnlyTransaction interface {
	Snapshot() ReadOnlyTransaction
	ListWorkspace(branch string) ([]*model.WorkspaceEntry, error)
	ReadFromWorkspace(branch, path string) (*model.WorkspaceEntry, error)
	ReadBranch(branch string) (*model.Branch, error)
	ReadObject(addr string) (*model.Object, error)
	ReadCommit(addr string) (*model.Commit, error)
	ListTree(addr string) ([]*model.Entry, error)
	ReadTreeEntry(treeAddress, name string, entryType model.Entry_Type) (*model.Entry, error)
}

type Transaction interface {
	ReadOnlyTransaction
	WriteToWorkspacePath(branch, path string, entry *model.WorkspaceEntry) error
	ClearWorkspace(branch string)
	WriteTree(address string, entries []*model.Entry) error
	WriteObject(addr string, object *model.Object) error
	WriteCommit(addr string, commit *model.Commit) error
	WriteRepo(repo *model.Repo) error
	WriteBranch(name string, branch *model.Branch) error
	DeleteBranch(name string)
}

type Store interface {
	Repos(clientId string) ([]*model.Repo, error)
	ReadTransact(*model.Repo, func(transaction ReadOnlyTransaction) (interface{}, error)) (interface{}, error)
	Transact(*model.Repo, func(Transaction) (interface{}, error)) (interface{}, error)
}

type fdbStore struct {
	db        fdb.Database
	repos     subspace.Subspace
	workspace subspace.Subspace
	entries   subspace.Subspace
	objects   subspace.Subspace
	commits   subspace.Subspace
	branches  subspace.Subspace
	refCounts subspace.Subspace
}

func (s *fdbStore) Repos(clientId string) ([]*model.Repo, error) {
	repos, err := s.db.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		q := s.repos.Pack(tuple.Tuple{clientId})
		iter := tx.GetRange(fdb.KeyRange{
			Begin: q,
			End:   append(q, 0xFF),
		}, fdb.RangeOptions{}).Iterator()
		repos := make([]*model.Repo, 0)
		for iter.Advance() {
			repoData := iter.MustGet()
			repo := &model.Repo{}
			err := proto.Unmarshal(repoData.Value, repo)
			if err != nil {
				return nil, err
			}
			repos = append(repos, repo)
		}
		return repos, nil
	})
	if err != nil {
		return nil, err
	}
	return repos.([]*model.Repo), nil
}

func (s *fdbStore) ReadTransact(repo *model.Repo, fn func(transaction ReadOnlyTransaction) (interface{}, error)) (interface{}, error) {
	return s.db.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		t := &fdbReadOnlyTransaction{
			repos:     s.repos,
			workspace: s.workspace,
			entries:   s.entries,
			objects:   s.objects,
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
				repos:     s.repos,
				workspace: s.workspace,
				entries:   s.entries,
				objects:   s.objects,
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
	repos     subspace.Subspace
	workspace subspace.Subspace
	trees     subspace.Subspace
	entries   subspace.Subspace
	objects   subspace.Subspace
	commits   subspace.Subspace
	branches  subspace.Subspace
	refCounts subspace.Subspace

	query readQuery
}

type fdbTransaction struct {
	fdbReadOnlyTransaction
	query query
}

func (s *fdbReadOnlyTransaction) Snapshot() ReadOnlyTransaction {
	return &fdbReadOnlyTransaction{
		workspace: s.workspace,
		trees:     s.trees,
		entries:   s.entries,
		objects:   s.objects,
		commits:   s.commits,
		branches:  s.branches,
		refCounts: s.refCounts,
		query: readQuery{
			repo: s.query.repo,
			tx:   s.query.tx.Snapshot(),
		},
	}
}

func (s *fdbReadOnlyTransaction) ListWorkspace(branch string) ([]*model.WorkspaceEntry, error) {
	iter := s.query.rangePrefix(s.workspace, branch)
	ws := make([]*model.WorkspaceEntry, 0)
	for iter.Advance() {
		kv := iter.MustGet()
		ent := &model.WorkspaceEntry{}
		err := proto.Unmarshal(kv.Value, ent)
		if err != nil {
			return nil, errors.ErrIndexMalformed
		}
		ws = append(ws, ent)
	}
	return ws, nil
}

func (s *fdbReadOnlyTransaction) ReadFromWorkspace(branch, path string) (*model.WorkspaceEntry, error) {
	// read the blob's hash addr
	data := s.query.get(s.workspace, branch, path).MustGet()
	if data == nil {
		return nil, errors.ErrNotFound
	}
	ent := &model.WorkspaceEntry{}
	err := proto.Unmarshal(data, ent)
	if err != nil {
		return nil, errors.ErrIndexMalformed
	}
	return ent, nil
}

func (s *fdbReadOnlyTransaction) ReadBranch(branch string) (*model.Branch, error) {
	// read branch attributes
	data := s.query.get(s.branches, branch).MustGet()
	if data == nil {
		return nil, errors.ErrNotFound
	}
	branchModel := &model.Branch{}
	err := proto.Unmarshal(data, branchModel)
	if err != nil {
		return nil, errors.ErrIndexMalformed
	}
	return branchModel, nil
}

func (s *fdbReadOnlyTransaction) ReadObject(addr string) (*model.Object, error) {
	objData := s.query.get(s.objects, addr).MustGet()
	if objData == nil {
		return nil, errors.ErrNotFound
	}
	obj := &model.Object{}
	err := proto.Unmarshal(objData, obj)
	if err != nil {
		return nil, errors.ErrIndexMalformed
	}
	return obj, nil
}

func (s *fdbReadOnlyTransaction) ReadCommit(addr string) (*model.Commit, error) {
	data := s.query.get(s.commits, addr).MustGet()
	if data == nil {
		return nil, errors.ErrNotFound
	}
	commit := &model.Commit{}
	err := proto.Unmarshal(data, commit)
	if err != nil {
		return nil, errors.ErrIndexMalformed
	}
	return commit, nil
}

func (s *fdbReadOnlyTransaction) ListTree(addr string) ([]*model.Entry, error) {
	iter := s.query.rangePrefix(s.entries, addr)
	entries := make([]*model.Entry, 0)
	for iter.Advance() {
		entryData := iter.MustGet()
		entry := &model.Entry{}
		err := proto.Unmarshal(entryData.Value, entry)
		if err != nil {
			return nil, errors.ErrIndexMalformed
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (s *fdbReadOnlyTransaction) ReadTreeEntry(treeAddress, name string, entryType model.Entry_Type) (*model.Entry, error) {
	data := s.query.get(s.entries, treeAddress, int(entryType), name).MustGet()
	if data == nil {
		return nil, errors.ErrNotFound
	}
	entry := &model.Entry{}
	err := proto.Unmarshal(data, entry)
	if err != nil {
		return nil, errors.ErrIndexMalformed
	}
	return entry, nil
}

func (s *fdbTransaction) WriteToWorkspacePath(branch, path string, entry *model.WorkspaceEntry) error {
	data, err := proto.Marshal(entry)
	if err != nil {
		return errors.ErrIndexMalformed
	}
	s.query.set(data, s.workspace, branch, path)
	return nil
}

func (s *fdbTransaction) ClearWorkspace(branch string) {
	s.query.clearPrefix(s.workspace, s.query.repo.GetClientId(), s.query.repo.GetRepoId(), branch)
}

func (s *fdbTransaction) WriteTree(address string, entries []*model.Entry) error {
	for _, entry := range entries {
		data, err := proto.Marshal(entry)
		if err != nil {
			return err
		}
		s.query.set(data, s.entries, address, int(entry.GetType()), entry.GetName())
	}
	return nil
}

func (s *fdbTransaction) WriteObject(addr string, object *model.Object) error {
	data, err := proto.Marshal(object)
	if err != nil {
		return err
	}
	s.query.set(data, s.objects, addr)
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

func (s *fdbTransaction) WriteRepo(repo *model.Repo) error {
	data, err := proto.Marshal(repo)
	if err != nil {
		return err
	}
	s.query.set(data, s.repos)
	return nil
}
