package store

import (
	"strconv"
	"treeverse-lake/db"
	"treeverse-lake/index/errors"
	"treeverse-lake/index/model"

	"github.com/golang/protobuf/proto"
)

type RepoReadOnlyOperations interface {
	ReadRepo() (*model.Repo, error)
	ListWorkspace(branch string) ([]*model.WorkspaceEntry, error)
	ReadFromWorkspace(branch, path string) (*model.WorkspaceEntry, error)
	ListBranches() ([]*model.Branch, error)
	ReadBranch(branch string) (*model.Branch, error)
	ReadObject(addr string) (*model.Object, error)
	ReadCommit(addr string) (*model.Commit, error)
	ListTree(addr, from string, results int) ([]*model.Entry, bool, error)
	ReadTreeEntry(treeAddress, name string, entryType model.Entry_Type) (*model.Entry, error)
}

type RepoOperations interface {
	RepoReadOnlyOperations
	WriteToWorkspacePath(branch, path string, entry *model.WorkspaceEntry) error
	ClearWorkspace(branch string)
	WriteTree(address string, entries []*model.Entry) error
	WriteObject(addr string, object *model.Object) error
	WriteCommit(addr string, commit *model.Commit) error
	WriteBranch(name string, branch *model.Branch) error
	DeleteBranch(name string)
	WriteRepo(repo *model.Repo) error
}

type KVRepoReadOnlyOperations struct {
	query  db.ReadQuery
	store  db.Store
	repoId string
}

type KVRepoOperations struct {
	*KVRepoReadOnlyOperations
	query db.Query
}

func (s *KVRepoReadOnlyOperations) ReadRepo() (*model.Repo, error) {
	repo := &model.Repo{}
	return repo, s.query.GetAsProto(repo, SubspaceRepos, db.CompositeStrings(s.repoId))
}

func (s *KVRepoReadOnlyOperations) ListWorkspace(branch string) ([]*model.WorkspaceEntry, error) {
	iter := s.query.RangePrefix(SubspaceWorkspace, db.CompositeStrings(s.repoId, branch))
	ws := make([]*model.WorkspaceEntry, 0)
	for iter.Advance() {
		kv, err := iter.Get()
		if err != nil {
			return nil, errors.ErrIndexMalformed
		}
		ent := &model.WorkspaceEntry{}
		err = proto.Unmarshal(kv.Value, ent)
		if err != nil {
			return nil, errors.ErrIndexMalformed
		}
		ws = append(ws, ent)
	}
	return ws, nil
}

func (s *KVRepoReadOnlyOperations) ReadFromWorkspace(branch, path string) (*model.WorkspaceEntry, error) {
	ent := &model.WorkspaceEntry{}
	return ent, s.query.GetAsProto(ent, SubspaceWorkspace, db.CompositeStrings(s.repoId, branch, path))
}

func (s *KVRepoReadOnlyOperations) ListBranches() ([]*model.Branch, error) {
	iter := s.query.RangePrefix(SubspaceBranches, db.CompositeStrings(s.repoId))
	branches := make([]*model.Branch, 0)
	for iter.Advance() {
		branch := &model.Branch{}
		kv, err := iter.Get()
		if err != nil {
			return nil, errors.ErrIndexMalformed
		}
		err = proto.Unmarshal(kv.Value, branch)
		if err != nil {
			return nil, errors.ErrIndexMalformed
		}
		branches = append(branches, branch)
	}
	return branches, nil
}

func (s *KVRepoReadOnlyOperations) ReadBranch(branch string) (*model.Branch, error) {
	b := &model.Branch{}
	return b, s.query.GetAsProto(b, SubspaceBranches, db.CompositeStrings(s.repoId, branch))
}

func (s *KVRepoReadOnlyOperations) ReadObject(addr string) (*model.Object, error) {
	obj := &model.Object{}
	return obj, s.query.GetAsProto(obj, SubspaceObjects, db.CompositeStrings(s.repoId, addr))
}

func (s *KVRepoReadOnlyOperations) ReadCommit(addr string) (*model.Commit, error) {
	commit := &model.Commit{}
	return commit, s.query.GetAsProto(commit, SubspaceCommits, db.CompositeStrings(s.repoId, addr))
}

func (s *KVRepoReadOnlyOperations) ListTree(addr, from string, results int) ([]*model.Entry, bool, error) {
	// entry keys: (client, repo, parent, name, type)
	var iter db.Iterator
	var hasMore bool
	if len(from) > 0 {
		iter = s.query.RangePrefixGreaterThan(SubspaceEntries, db.CompositeStrings(s.repoId, addr), []byte(from))
	} else {
		iter = s.query.RangePrefix(SubspaceEntries, db.CompositeStrings(s.repoId, addr))
	}
	entries := make([]*model.Entry, 0)
	current := 0
	for iter.Advance() {
		entryData, err := iter.Get()
		if err != nil {
			return nil, false, errors.ErrIndexMalformed
		}
		entry := &model.Entry{}
		err = proto.Unmarshal(entryData.Value, entry)
		if err != nil {
			return nil, false, errors.ErrIndexMalformed
		}
		entries = append(entries, entry)
		current++
		if results != -1 && current >= results {
			hasMore = iter.Advance() // will return true if the iterator still has values to read
			break
		}
	}
	return entries, hasMore, nil
}

func (s *KVRepoReadOnlyOperations) ReadTreeEntry(treeAddress, name string, entryType model.Entry_Type) (*model.Entry, error) {
	entry := &model.Entry{}
	return entry, s.query.GetAsProto(entry, SubspaceEntries, db.CompositeStrings(s.repoId, treeAddress, name, strconv.Itoa(int(entryType))))
}

func (s *KVRepoOperations) WriteToWorkspacePath(branch, path string, entry *model.WorkspaceEntry) error {
	return s.query.SetProto(entry, SubspaceWorkspace, db.CompositeStrings(s.repoId, branch, path))
}

func (s *KVRepoOperations) ClearWorkspace(branch string) {
	s.query.ClearPrefix(SubspaceWorkspace, db.CompositeStrings(s.repoId, branch))
}

func (s *KVRepoOperations) WriteTree(address string, entries []*model.Entry) error {
	for _, entry := range entries {
		err := s.query.SetProto(entry, SubspaceEntries, db.CompositeStrings(s.repoId, address, entry.GetName(), strconv.Itoa(int(entry.GetType()))))
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *KVRepoOperations) WriteObject(addr string, object *model.Object) error {
	return s.query.SetProto(object, SubspaceObjects, db.CompositeStrings(s.repoId, addr))
}

func (s *KVRepoOperations) WriteCommit(addr string, commit *model.Commit) error {
	return s.query.SetProto(commit, SubspaceCommits, db.CompositeStrings(s.repoId, addr))
}

func (s *KVRepoOperations) WriteBranch(name string, branch *model.Branch) error {
	return s.query.SetProto(branch, SubspaceBranches, db.CompositeStrings(s.repoId, name))
}

func (s *KVRepoOperations) DeleteBranch(name string) {
	s.query.Delete(SubspaceBranches, db.CompositeStrings(s.repoId, name))
}

func (s *KVRepoOperations) WriteRepo(repo *model.Repo) error {
	return s.query.SetProto(repo, SubspaceRepos, db.CompositeStrings(s.repoId))
}
