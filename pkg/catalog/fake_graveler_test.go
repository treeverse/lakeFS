package catalog

import (
	"bytes"
	"context"
	"strings"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type FakeGraveler struct {
	graveler.VersionController
	KeyValue                  map[string]*graveler.Value
	Err                       error
	ListIteratorFactory       func() graveler.ValueIterator
	DiffIteratorFactory       func() graveler.DiffIterator
	RepositoryIteratorFactory func() graveler.RepositoryIterator
	BranchIteratorFactory     func() graveler.BranchIterator
	TagIteratorFactory        func() graveler.TagIterator
	hooks                     graveler.HooksHandler
}

func (g *FakeGraveler) ParseRef(ref graveler.Ref) (graveler.RawRef, error) {
	panic("implement me")
}

func (g *FakeGraveler) ResolveRawRef(ctx context.Context, repository *graveler.RepositoryRecord, rawRef graveler.RawRef) (*graveler.ResolvedRef, error) {
	panic("implement me")
}

func (g *FakeGraveler) SaveGarbageCollectionCommits(ctx context.Context, repository *graveler.RepositoryRecord, previousRunID string) (garbageCollectionRunMetadata *graveler.GarbageCollectionRunMetadata, err error) {
	panic("implement me")
}

func (g *FakeGraveler) GetGarbageCollectionRules(ctx context.Context, repository *graveler.RepositoryRecord) (*graveler.GarbageCollectionRules, error) {
	panic("implement me")
}

func (g *FakeGraveler) SetGarbageCollectionRules(ctx context.Context, repository *graveler.RepositoryRecord, rules *graveler.GarbageCollectionRules) error {
	panic("implement me")
}

func (g *FakeGraveler) CreateBareRepository(ctx context.Context, repositoryID graveler.RepositoryID, storageNamespace graveler.StorageNamespace, branchID graveler.BranchID) (*graveler.RepositoryRecord, error) {
	panic("implement me")
}

func (g *FakeGraveler) LoadCommits(ctx context.Context, repository *graveler.RepositoryRecord, metaRangeID graveler.MetaRangeID) error {
	panic("implement me")
}

func (g *FakeGraveler) LoadBranches(ctx context.Context, repository *graveler.RepositoryRecord, metaRangeID graveler.MetaRangeID) error {
	panic("implement me")
}

func (g *FakeGraveler) LoadTags(ctx context.Context, repository *graveler.RepositoryRecord, metaRangeID graveler.MetaRangeID) error {
	panic("implement me")
}

func (g *FakeGraveler) DumpCommits(ctx context.Context, repository *graveler.RepositoryRecord) (*graveler.MetaRangeID, error) {
	panic("implement me")
}

func (g *FakeGraveler) DumpBranches(ctx context.Context, repository *graveler.RepositoryRecord) (*graveler.MetaRangeID, error) {
	panic("implement me")
}

func (g *FakeGraveler) DumpTags(ctx context.Context, repository *graveler.RepositoryRecord) (*graveler.MetaRangeID, error) {
	panic("implement me")
}

func (g *FakeGraveler) GetMetaRange(ctx context.Context, repository *graveler.RepositoryRecord, metaRangeID graveler.MetaRangeID) (graveler.MetaRangeAddress, error) {
	panic("implement me")
}

func (g *FakeGraveler) GetRange(ctx context.Context, repository *graveler.RepositoryRecord, rangeID graveler.RangeID) (graveler.RangeAddress, error) {
	panic("implement me")
}

func fakeGravelerBuildKey(repositoryID graveler.RepositoryID, ref graveler.Ref, key graveler.Key) string {
	return strings.Join([]string{repositoryID.String(), ref.String(), key.String()}, "/")
}

func (g *FakeGraveler) Get(_ context.Context, repository *graveler.RepositoryRecord, ref graveler.Ref, key graveler.Key) (*graveler.Value, error) {
	if g.Err != nil {
		return nil, g.Err
	}
	k := fakeGravelerBuildKey(repository.RepositoryID, ref, key)
	v := g.KeyValue[k]
	if v == nil {
		return nil, graveler.ErrNotFound
	}
	return v, nil
}

func (g *FakeGraveler) GetByCommitID(ctx context.Context, repository *graveler.RepositoryRecord, commitID graveler.CommitID, key graveler.Key) (*graveler.Value, error) {
	return g.Get(ctx, repository, graveler.Ref(commitID), key)
}

func (g *FakeGraveler) Set(_ context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, key graveler.Key, value graveler.Value, _ ...graveler.WriteConditionOption) error {
	if g.Err != nil {
		return g.Err
	}
	k := fakeGravelerBuildKey(repository.RepositoryID, graveler.Ref(branchID.String()), key)
	g.KeyValue[k] = &value
	return nil
}

func (g *FakeGraveler) Delete(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, key graveler.Key) error {
	panic("implement me")
}

func (g *FakeGraveler) List(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.Ref) (graveler.ValueIterator, error) {
	if g.Err != nil {
		return nil, g.Err
	}
	return g.ListIteratorFactory(), nil
}

func (g *FakeGraveler) GetRepository(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.RepositoryRecord, error) {
	return &graveler.RepositoryRecord{RepositoryID: repositoryID}, nil
}

func (g *FakeGraveler) CreateRepository(ctx context.Context, repositoryID graveler.RepositoryID, storageNamespace graveler.StorageNamespace, branchID graveler.BranchID) (*graveler.RepositoryRecord, error) {
	panic("implement me")
}

func (g *FakeGraveler) ListRepositories(ctx context.Context) (graveler.RepositoryIterator, error) {
	if g.Err != nil {
		return nil, g.Err
	}
	return g.RepositoryIteratorFactory(), nil
}

func (g *FakeGraveler) DeleteRepository(ctx context.Context, repositoryID graveler.RepositoryID) error {
	panic("implement me")
}

func (g *FakeGraveler) CreateBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, ref graveler.Ref) (*graveler.Branch, error) {
	panic("implement me")
}

func (g *FakeGraveler) UpdateBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, ref graveler.Ref) (*graveler.Branch, error) {
	panic("implement me")
}

func (g *FakeGraveler) GetBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID) (*graveler.Branch, error) {
	if g.Err != nil {
		return nil, g.Err
	}
	it := g.BranchIteratorFactory()
	// TODO(nopcoder): handle repositoryID
	it.SeekGE(branchID)
	if it.Err() != nil {
		return nil, it.Err()
	}
	if !it.Next() {
		return nil, graveler.ErrNotFound
	}
	branch := it.Value()
	if branch.BranchID != branchID {
		return nil, graveler.ErrNotFound
	}
	return &graveler.Branch{CommitID: branch.CommitID, StagingToken: branch.StagingToken}, nil
}

func (g *FakeGraveler) GetTag(ctx context.Context, repository *graveler.RepositoryRecord, tagID graveler.TagID) (*graveler.CommitID, error) {
	panic("implement me")
}

func (g *FakeGraveler) CreateTag(ctx context.Context, repository *graveler.RepositoryRecord, tagID graveler.TagID, commitID graveler.CommitID) error {
	panic("implement me")
}

func (g *FakeGraveler) DeleteTag(ctx context.Context, repository *graveler.RepositoryRecord, tagID graveler.TagID) error {
	panic("implement me")
}

func (g *FakeGraveler) ListTags(ctx context.Context, repository *graveler.RepositoryRecord) (graveler.TagIterator, error) {
	if g.Err != nil {
		return nil, g.Err
	}
	return g.TagIteratorFactory(), nil
}

func (g *FakeGraveler) Log(ctx context.Context, repository *graveler.RepositoryRecord, commitID graveler.CommitID) (graveler.CommitIterator, error) {
	panic("implement me")
}

func (g *FakeGraveler) ListBranches(_ context.Context, _ *graveler.RepositoryRecord) (graveler.BranchIterator, error) {
	if g.Err != nil {
		return nil, g.Err
	}
	return g.BranchIteratorFactory(), nil
}

func (g *FakeGraveler) DeleteBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID) error {
	panic("implement me")
}

func (g *FakeGraveler) Commit(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, _ graveler.CommitParams) (graveler.CommitID, error) {
	panic("implement me")
}

func (g *FakeGraveler) WriteRange(ctx context.Context, repository *graveler.RepositoryRecord, it graveler.ValueIterator) (*graveler.RangeInfo, error) {
	panic("implement me")
}

func (g *FakeGraveler) WriteMetaRange(ctx context.Context, repository *graveler.RepositoryRecord, ranges []*graveler.RangeInfo) (*graveler.MetaRangeInfo, error) {
	panic("implement me")
}

func (g *FakeGraveler) GetCommit(ctx context.Context, repository *graveler.RepositoryRecord, commitID graveler.CommitID) (*graveler.Commit, error) {
	panic("implement me")
}

func (g *FakeGraveler) Dereference(ctx context.Context, repository *graveler.RepositoryRecord, ref graveler.Ref) (*graveler.ResolvedRef, error) {
	panic("implement me")
}

func (g *FakeGraveler) Reset(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID) error {
	panic("implement me")
}

func (g *FakeGraveler) ResetKey(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, key graveler.Key) error {
	panic("implement me")
}

func (g *FakeGraveler) ResetPrefix(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, key graveler.Key) error {
	panic("implement me")
}

func (g *FakeGraveler) Revert(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, _ graveler.Ref, _ int, _ graveler.CommitParams) (graveler.CommitID, error) {
	panic("implement me")
}

func (g *FakeGraveler) Merge(ctx context.Context, repository *graveler.RepositoryRecord, destination graveler.BranchID, source graveler.Ref, _ graveler.CommitParams, strategy string) (graveler.CommitID, error) {
	panic("implement me")
}

func (g *FakeGraveler) DiffUncommitted(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID) (graveler.DiffIterator, error) {
	if g.Err != nil {
		return nil, g.Err
	}
	return g.DiffIteratorFactory(), nil
}

func (g *FakeGraveler) Diff(_ context.Context, _ *graveler.RepositoryRecord, _, _ graveler.Ref) (graveler.DiffIterator, error) {
	if g.Err != nil {
		return nil, g.Err
	}
	return g.DiffIteratorFactory(), nil
}

func (g *FakeGraveler) Compare(_ context.Context, _ *graveler.RepositoryRecord, _, _ graveler.Ref) (graveler.DiffIterator, error) {
	if g.Err != nil {
		return nil, g.Err
	}
	return g.DiffIteratorFactory(), nil
}

func (g *FakeGraveler) SetHooksHandler(handler graveler.HooksHandler) {
	g.hooks = handler
}

func (g *FakeGraveler) AddCommitToBranchHead(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, commit graveler.Commit) (graveler.CommitID, error) {
	panic("implement me")
}

func (g *FakeGraveler) AddCommit(ctx context.Context, repository *graveler.RepositoryRecord, commit graveler.Commit) (graveler.CommitID, error) {
	panic("implement me")
}

func (g *FakeGraveler) AddCommitNoLock(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.Commit) (graveler.CommitID, error) {
	panic("implement me")
}

func (g *FakeGraveler) WriteMetaRangeByIterator(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.ValueIterator) (*graveler.MetaRangeID, error) {
	panic("implement me")
}

func (g *FakeGraveler) GetStagingToken(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID) (*graveler.StagingToken, error) {
	panic("implement me")
}

type FakeValueIterator struct {
	Data  []*graveler.ValueRecord
	Index int
	Error error
}

func NewFakeValueIterator(data []*graveler.ValueRecord) *FakeValueIterator {
	return &FakeValueIterator{Data: data, Index: -1}
}

func NewFakeValueIteratorFactory(data []*graveler.ValueRecord) func() graveler.ValueIterator {
	return func() graveler.ValueIterator {
		return NewFakeValueIterator(data)
	}
}

func (m *FakeValueIterator) Next() bool {
	if m.Index >= len(m.Data) {
		return false
	}
	m.Index++
	return m.Index < len(m.Data)
}

func (m *FakeValueIterator) SeekGE(id graveler.Key) {
	m.Index = len(m.Data)
	for i, d := range m.Data {
		if bytes.Compare(d.Key, id) >= 0 {
			m.Index = i - 1
			return
		}
	}
}

func (m *FakeValueIterator) Value() *graveler.ValueRecord {
	return m.Data[m.Index]
}

func (m *FakeValueIterator) Err() error {
	return nil
}

func (m *FakeValueIterator) Close() {}

type FakeDiffIterator struct {
	Data  []*graveler.Diff
	Index int
}

func NewFakeDiffIterator(data []*graveler.Diff) *FakeDiffIterator {
	return &FakeDiffIterator{Data: data, Index: -1}
}

func NewFakeDiffIteratorFactory(data []*graveler.Diff) func() graveler.DiffIterator {
	return func() graveler.DiffIterator {
		return NewFakeDiffIterator(data)
	}
}

func (m *FakeDiffIterator) Next() bool {
	if m.Index >= len(m.Data) {
		return false
	}
	m.Index++
	return m.Index < len(m.Data)
}

func (m *FakeDiffIterator) SeekGE(_ graveler.Key) {
	panic("implement me")
}

func (m *FakeDiffIterator) Value() *graveler.Diff {
	return m.Data[m.Index]
}

func (m *FakeDiffIterator) Err() error {
	return nil
}

func (m *FakeDiffIterator) Close() {}

type FakeRepositoryIterator struct {
	Data  []*graveler.RepositoryRecord
	Index int
}

func NewFakeRepositoryIterator(data []*graveler.RepositoryRecord) *FakeRepositoryIterator {
	return &FakeRepositoryIterator{Data: data, Index: -1}
}

func NewFakeRepositoryIteratorFactory(data []*graveler.RepositoryRecord) func() graveler.RepositoryIterator {
	return func() graveler.RepositoryIterator {
		return NewFakeRepositoryIterator(data)
	}
}

func (m *FakeRepositoryIterator) Next() bool {
	if m.Index >= len(m.Data) {
		return false
	}
	m.Index++
	return m.Index < len(m.Data)
}

func (m *FakeRepositoryIterator) SeekGE(id graveler.RepositoryID) {
	m.Index = len(m.Data)
	for i, repo := range m.Data {
		if repo.RepositoryID >= id {
			m.Index = i - 1
			return
		}
	}
}

func (m *FakeRepositoryIterator) Value() *graveler.RepositoryRecord {
	return m.Data[m.Index]
}

func (m *FakeRepositoryIterator) Err() error {
	return nil
}

func (m *FakeRepositoryIterator) Close() {}

type FakeTagIterator struct {
	Data  []*graveler.TagRecord
	Index int
}

func NewFakeTagIterator(data []*graveler.TagRecord) *FakeTagIterator {
	return &FakeTagIterator{Data: data, Index: -1}
}

func NewFakeTagIteratorFactory(data []*graveler.TagRecord) func() graveler.TagIterator {
	return func() graveler.TagIterator { return NewFakeTagIterator(data) }
}

func (m *FakeTagIterator) Next() bool {
	if m.Index >= len(m.Data) {
		return false
	}
	m.Index++
	return m.Index < len(m.Data)
}

func (m *FakeTagIterator) SeekGE(id graveler.TagID) {
	m.Index = len(m.Data)
	for i, item := range m.Data {
		if item.TagID >= id {
			m.Index = i - 1
			return
		}
	}
}

func (m *FakeTagIterator) Value() *graveler.TagRecord {
	return m.Data[m.Index]
}

func (m *FakeTagIterator) Err() error {
	return nil
}

func (m *FakeTagIterator) Close() {}
