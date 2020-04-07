package store

import (
	"fmt"

	"golang.org/x/xerrors"

	"github.com/treeverse/lakefs/ident"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/index/errors"
	"github.com/treeverse/lakefs/index/model"

	"github.com/golang/protobuf/proto"
)

type RepoReadOnlyOperations interface {
	ReadRepo() (*model.Repo, error)
	ListWorkspace(branch string) ([]*model.WorkspaceEntry, error)
	ReadFromWorkspace(branch, path string) (*model.WorkspaceEntry, error)
	ListBranches(prefix string, amount int, after string) ([]*model.Branch, bool, error)
	ReadBranch(branch string) (*model.Branch, error)
	ReadRoot(addr string) (*model.Root, error)
	ReadObject(addr string) (*model.Object, error)
	ReadCommit(addr string) (*model.Commit, error)
	ListTree(addr, after string, results int) ([]*model.Entry, bool, error)
	ListTreeWithPrefix(addr, prefix, after string, results int) ([]*model.Entry, bool, error)
	ReadTreeEntry(treeAddress, name string) (*model.Entry, error)

	// Multipart uploads
	ReadMultipartUpload(uploadId string) (*model.MultipartUpload, error)
	ReadMultipartUploadPart(uploadId string, partNumber int) (*model.MultipartUploadPart, error)
	ListMultipartUploads() ([]*model.MultipartUpload, error)
	ListMultipartUploadParts(uploadId string) ([]*model.MultipartUploadPart, error)
	GetObjectDedup(DedupId []byte) (*model.ObjectDedup, error)
}

type RepoOperations interface {
	RepoReadOnlyOperations
	DeleteWorkspacePath(branch, path string) error
	WriteToWorkspacePath(branch, path string, entry *model.WorkspaceEntry) error
	ClearWorkspace(branch string) error
	WriteTree(address string, entries []*model.Entry) error
	WriteRoot(address string, root *model.Root) error
	WriteObject(addr string, object *model.Object) error
	WriteCommit(addr string, commit *model.Commit) error
	WriteBranch(name string, branch *model.Branch) error
	DeleteBranch(name string) error
	WriteRepo(repo *model.Repo) error

	// Multipart uploads
	WriteMultipartUpload(upload *model.MultipartUpload) error
	WriteMultipartUploadPart(uploadId string, partNumber int, part *model.MultipartUploadPart) error
	DeleteMultipartUpload(uploadId, path string) error
	DeleteMultipartUploadParts(uploadId string) error
	WriteObjectDedup(dedup *model.ObjectDedup) error
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
	iter, itclose := s.query.RangePrefix(SubspaceWorkspace, db.CompositeStrings(s.repoId, branch))
	defer itclose()
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

func (s *KVRepoReadOnlyOperations) ListBranches(prefix string, amount int, after string) ([]*model.Branch, bool, error) {
	var iter db.Iterator
	var itclose db.IteratorCloseFn
	prefixKey := db.CompositeStrings(s.repoId, prefix)
	if len(after) == 0 {
		iter, itclose = s.query.RangePrefix(SubspaceBranches, prefixKey)
	} else {
		iter, itclose = s.query.RangePrefixGreaterThan(SubspaceBranches, prefixKey, db.CompositeStrings(s.repoId, after))
	}
	defer itclose()
	branches := make([]*model.Branch, 0)
	var curr int
	var done, hasMore bool
	for iter.Advance() {
		if done {
			hasMore = true
			break
		}
		branch := &model.Branch{}
		kv, err := iter.Get()
		if err != nil {
			return nil, false, errors.ErrIndexMalformed
		}
		err = proto.Unmarshal(kv.Value, branch)
		if err != nil {
			return nil, false, errors.ErrIndexMalformed
		}
		branches = append(branches, branch)
		curr++
		if curr == amount {
			done = true
		}
	}
	return branches, hasMore, nil
}

func (s *KVRepoReadOnlyOperations) ReadBranch(branch string) (*model.Branch, error) {
	b := &model.Branch{}
	err := s.query.GetAsProto(b, SubspaceBranches, db.CompositeStrings(s.repoId, branch))
	if xerrors.Is(err, db.ErrNotFound) {
		err = errors.ErrBranchNotFound
	}
	return b, err
}

func (s *KVRepoReadOnlyOperations) ReadRoot(address string) (*model.Root, error) {
	r := &model.Root{}
	return r, s.query.GetAsProto(r, SubspaceRoots, db.CompositeStrings(s.repoId, address))
}

func (s *KVRepoReadOnlyOperations) ReadObject(addr string) (*model.Object, error) {
	obj := &model.Object{}
	return obj, s.query.GetAsProto(obj, SubspaceObjects, db.CompositeStrings(s.repoId, addr))
}

func (s *KVRepoReadOnlyOperations) ReadCommit(addr string) (*model.Commit, error) {
	commit := &model.Commit{}
	if len(addr) == ident.HashHexLength {
		return commit, s.query.GetAsProto(commit, SubspaceCommits, db.CompositeStrings(s.repoId, addr))
	}
	// otherwise, it could be a truncated commit hash - we support this as long as the results are not ambiguous
	// i.e. a truncated commit returns 1 - and only 1 result.
	iter, close := s.query.RangePrefix(SubspaceCommits, db.CompositeStrings(s.repoId, addr))
	defer close()
	var hasMatch bool
	var edata []byte
	for iter.Advance() {
		entryData, err := iter.Get()
		if err != nil {
			return nil, errors.ErrIndexMalformed
		}
		if hasMatch {
			// we have more than one commit matching the given prefix, this is ambiguous
			return nil, db.ErrNotFound
		}
		edata = entryData.Value
		hasMatch = true
	}
	if !hasMatch {
		// our iterator had no matches
		return nil, db.ErrNotFound
	}
	err := proto.Unmarshal(edata, commit)
	if err != nil {
		return nil, errors.ErrIndexMalformed
	}
	return commit, nil
}

func (s *KVRepoReadOnlyOperations) ListTreeWithPrefix(addr, prefix, after string, results int) ([]*model.Entry, bool, error) {
	var iter db.Iterator
	var itclose db.IteratorCloseFn
	var hasMore bool
	key := db.CompositeStrings(s.repoId, addr)
	if len(prefix) > 0 {
		key = key.With([]byte(prefix))
	}
	if len(after) > 0 {
		gtValue := db.CompositeStrings(s.repoId, addr, after)
		iter, itclose = s.query.RangePrefixGreaterThan(SubspaceEntries, key, gtValue)
	} else {
		iter, itclose = s.query.RangePrefix(SubspaceEntries, key)
	}
	defer itclose()
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

func (s *KVRepoReadOnlyOperations) ListTree(addr, after string, results int) ([]*model.Entry, bool, error) {
	return s.ListTreeWithPrefix(addr, "", after, results)
}

func (s *KVRepoReadOnlyOperations) ReadTreeEntry(treeAddress, name string) (*model.Entry, error) {
	entry := &model.Entry{}
	return entry, s.query.GetAsProto(entry, SubspaceEntries, db.CompositeStrings(s.repoId, treeAddress, name))
}

func (s *KVRepoReadOnlyOperations) ReadMultipartUpload(uploadId string) (*model.MultipartUpload, error) {
	m := &model.MultipartUpload{}
	return m, s.query.GetAsProto(m, SubspacesMultipartUploads, db.CompositeStrings(s.repoId, uploadId))
}

func (s *KVRepoReadOnlyOperations) GetObjectDedup(DedupId []byte) (*model.ObjectDedup, error) {
	m := &model.ObjectDedup{}
	return m, s.query.GetAsProto(m, SubspacesDedup, db.CompositeBytes([]byte(s.repoId), DedupId))
}

func (s *KVRepoReadOnlyOperations) ReadMultipartUploadPart(uploadId string, partNumber int) (*model.MultipartUploadPart, error) {
	m := &model.MultipartUploadPart{}
	partNumSortable := fmt.Sprintf("%.4d", partNumber) // allow up to 10k parts
	return m, s.query.GetAsProto(m, SubspacesMultipartUploadParts, db.CompositeStrings(s.repoId, uploadId, partNumSortable))
}

func (s *KVRepoReadOnlyOperations) ListMultipartUploads() ([]*model.MultipartUpload, error) {
	iter, itclose := s.query.RangePrefix(SubspacesMultipartUploads, db.CompositeStrings(s.repoId))
	defer itclose()
	uploads := make([]*model.MultipartUpload, 0)
	for iter.Advance() {
		entryData, err := iter.Get()
		if err != nil {
			return nil, errors.ErrIndexMalformed
		}
		m := &model.MultipartUpload{}
		err = proto.Unmarshal(entryData.Value, m)
		if err != nil {
			return nil, errors.ErrIndexMalformed
		}
		uploads = append(uploads, m)
	}
	return uploads, nil
}

func (s *KVRepoReadOnlyOperations) ListMultipartUploadParts(uploadId string) ([]*model.MultipartUploadPart, error) {
	iter, iterclose := s.query.RangePrefix(SubspacesMultipartUploadParts, db.CompositeStrings(s.repoId, uploadId))
	defer iterclose()
	parts := make([]*model.MultipartUploadPart, 0)
	for iter.Advance() {
		data, err := iter.Get()
		if err != nil {
			return nil, errors.ErrIndexMalformed
		}
		m := &model.MultipartUploadPart{}
		err = proto.Unmarshal(data.Value, m)
		if err != nil {
			return nil, errors.ErrIndexMalformed
		}
		parts = append(parts, m)
	}
	return parts, nil
}

func (s *KVRepoOperations) DeleteWorkspacePath(branch, path string) error {
	return s.query.Delete(SubspaceWorkspace, db.CompositeStrings(s.repoId, branch, path))
}
func (s *KVRepoOperations) WriteToWorkspacePath(branch, path string, entry *model.WorkspaceEntry) error {
	return s.query.SetProto(entry, SubspaceWorkspace, db.CompositeStrings(s.repoId, branch, path))
}

func (s *KVRepoOperations) WriteObjectDedup(dedup *model.ObjectDedup) error {
	return s.query.SetProto(dedup, SubspacesDedup, db.CompositeBytes([]byte(s.repoId), dedup.DedupId))
}

func (s *KVRepoOperations) ClearWorkspace(branch string) error {
	return s.query.ClearPrefix(SubspaceWorkspace, db.CompositeStrings(s.repoId, branch))
}

func (s *KVRepoOperations) WriteTree(address string, entries []*model.Entry) error {
	for _, entry := range entries {
		err := s.query.SetProto(entry, SubspaceEntries, db.CompositeStrings(s.repoId, address, entry.GetName()))
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *KVRepoOperations) WriteRoot(address string, root *model.Root) error {
	return s.query.SetProto(root, SubspaceRoots, db.CompositeStrings(s.repoId, address))
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

func (s *KVRepoOperations) DeleteBranch(name string) error {
	return s.query.Delete(SubspaceBranches, db.CompositeStrings(s.repoId, name))
}

func (s *KVRepoOperations) WriteMultipartUpload(upload *model.MultipartUpload) error {
	return s.query.SetProto(upload, SubspacesMultipartUploads, db.CompositeStrings(s.repoId, upload.GetId()))
}

func (s *KVRepoOperations) WriteRepo(repo *model.Repo) error {
	return s.query.SetProto(repo, SubspaceRepos, db.CompositeStrings(s.repoId))
}

func (s *KVRepoOperations) WriteMultipartUploadPart(uploadId string, partNumber int, part *model.MultipartUploadPart) error {
	// convert part to a string sortable representation
	partNumSortable := fmt.Sprintf("%.4d", partNumber) // allow up to 10k parts
	return s.query.SetProto(part, SubspacesMultipartUploadParts, db.CompositeStrings(s.repoId, uploadId, partNumSortable))
}

func (s *KVRepoOperations) DeleteMultipartUpload(uploadId, path string) error {
	return s.query.Delete(SubspacesMultipartUploads, db.CompositeStrings(s.repoId, uploadId))
}

func (s *KVRepoOperations) DeleteMultipartUploadParts(uploadId string) error {
	return s.query.ClearPrefix(SubspacesMultipartUploadParts, db.CompositeStrings(s.repoId, uploadId))
}
