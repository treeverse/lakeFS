package catalog

import (
	"bytes"
	"context"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
)

type FakeGraveler struct {
	graveler.VersionController
	KeyValue                   map[string]*graveler.Value
	Err                        error
	ListIteratorFactory        func() graveler.ValueIterator
	DiffIteratorFactory        func() graveler.DiffIterator
	RepositoryIteratorFactory  func() graveler.RepositoryIterator
	BranchIteratorFactory      func() graveler.BranchIterator
	TagIteratorFactory         func() graveler.TagIterator
	LinkAddressIteratorFactory func() graveler.LinkAddressIterator
	hooks                      graveler.HooksHandler
}

func (g *FakeGraveler) StageObject(ctx context.Context, stagingToken string, object graveler.ValueRecord) error {
	// TODO implement me
	panic("implement me")
}

func (g *FakeGraveler) GetRangeIDByKey(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.CommitID, _ graveler.Key) (graveler.RangeID, error) {
	panic("implement me")
}

func (g *FakeGraveler) ParseRef(_ graveler.Ref) (graveler.RawRef, error) {
	panic("implement me")
}

func (g *FakeGraveler) ResolveRawRef(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.RawRef) (*graveler.ResolvedRef, error) {
	panic("implement me")
}

func (g *FakeGraveler) SaveGarbageCollectionCommits(_ context.Context, _ *graveler.RepositoryRecord) (garbageCollectionRunMetadata *graveler.GarbageCollectionRunMetadata, err error) {
	panic("implement me")
}

func (g *FakeGraveler) GetGarbageCollectionRules(_ context.Context, _ *graveler.RepositoryRecord) (*graveler.GarbageCollectionRules, error) {
	panic("implement me")
}

func (g *FakeGraveler) SetGarbageCollectionRules(_ context.Context, _ *graveler.RepositoryRecord, _ *graveler.GarbageCollectionRules) error {
	panic("implement me")
}

func (g *FakeGraveler) CreateBareRepository(_ context.Context, _ graveler.RepositoryID, _ graveler.StorageID, _ graveler.StorageNamespace, _ graveler.BranchID, _ bool) (*graveler.RepositoryRecord, error) {
	panic("implement me")
}

func (g *FakeGraveler) LoadCommits(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.MetaRangeID, _ ...graveler.SetOptionsFunc) error {
	panic("implement me")
}

func (g *FakeGraveler) LoadBranches(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.MetaRangeID, _ ...graveler.SetOptionsFunc) error {
	panic("implement me")
}

func (g *FakeGraveler) LoadTags(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.MetaRangeID, _ ...graveler.SetOptionsFunc) error {
	panic("implement me")
}

func (g *FakeGraveler) DumpCommits(_ context.Context, _ *graveler.RepositoryRecord) (*graveler.MetaRangeID, error) {
	panic("implement me")
}

func (g *FakeGraveler) DumpBranches(_ context.Context, _ *graveler.RepositoryRecord) (*graveler.MetaRangeID, error) {
	panic("implement me")
}

func (g *FakeGraveler) DumpTags(_ context.Context, _ *graveler.RepositoryRecord) (*graveler.MetaRangeID, error) {
	panic("implement me")
}

func (g *FakeGraveler) GetMetaRange(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.MetaRangeID) (graveler.MetaRangeAddress, error) {
	panic("implement me")
}

func (g *FakeGraveler) GetRange(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.RangeID) (graveler.RangeAddress, error) {
	panic("implement me")
}

func fakeGravelerBuildKey(repositoryID graveler.RepositoryID, ref graveler.Ref, key graveler.Key) string {
	return strings.Join([]string{repositoryID.String(), ref.String(), key.String()}, "/")
}

func (g *FakeGraveler) Get(_ context.Context, repository *graveler.RepositoryRecord, ref graveler.Ref, key graveler.Key, opts ...graveler.GetOptionsFunc) (*graveler.Value, error) {
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

func (g *FakeGraveler) Set(_ context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, key graveler.Key, value graveler.Value, _ ...graveler.SetOptionsFunc) error {
	if g.Err != nil {
		return g.Err
	}
	k := fakeGravelerBuildKey(repository.RepositoryID, graveler.Ref(branchID.String()), key)
	g.KeyValue[k] = &value
	return nil
}

func (g *FakeGraveler) Update(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, key graveler.Key, update graveler.ValueUpdateFunc, opts ...graveler.SetOptionsFunc) error {
	if g.Err != nil {
		return g.Err
	}
	k := fakeGravelerBuildKey(repository.RepositoryID, graveler.Ref(branchID.String()), key)
	value, err := update(g.KeyValue[k])
	if err != nil {
		return err
	}
	g.KeyValue[k] = value
	return nil
}

func (g *FakeGraveler) Delete(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, key graveler.Key, _ ...graveler.SetOptionsFunc) error {
	return nil
}

func (g *FakeGraveler) DeleteBatch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, keys []graveler.Key, _ ...graveler.SetOptionsFunc) error {
	return nil
}

func (g *FakeGraveler) List(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.Ref, _ int) (graveler.ValueIterator, error) {
	if g.Err != nil {
		return nil, g.Err
	}
	return g.ListIteratorFactory(), nil
}

func (g *FakeGraveler) GetRepository(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.RepositoryRecord, error) {
	return &graveler.RepositoryRecord{RepositoryID: repositoryID}, nil
}

func (g *FakeGraveler) CreateRepository(ctx context.Context, repositoryID graveler.RepositoryID, storageID graveler.StorageID, storageNamespace graveler.StorageNamespace, branchID graveler.BranchID, readOnly bool) (*graveler.RepositoryRecord, error) {
	panic("implement me")
}

func (g *FakeGraveler) ListRepositories(ctx context.Context) (graveler.RepositoryIterator, error) {
	if g.Err != nil {
		return nil, g.Err
	}
	return g.RepositoryIteratorFactory(), nil
}

func (g *FakeGraveler) DeleteRepository(ctx context.Context, repositoryID graveler.RepositoryID, opts ...graveler.SetOptionsFunc) error {
	panic("implement me")
}

func (g *FakeGraveler) CreateBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, ref graveler.Ref, _ ...graveler.SetOptionsFunc) (*graveler.Branch, error) {
	panic("implement me")
}

func (g *FakeGraveler) UpdateBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, ref graveler.Ref, _ ...graveler.SetOptionsFunc) (*graveler.Branch, error) {
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

func (g *FakeGraveler) CreateTag(ctx context.Context, repository *graveler.RepositoryRecord, tagID graveler.TagID, commitID graveler.CommitID, _ ...graveler.SetOptionsFunc) error {
	panic("implement me")
}

func (g *FakeGraveler) DeleteTag(ctx context.Context, repository *graveler.RepositoryRecord, tagID graveler.TagID, _ ...graveler.SetOptionsFunc) error {
	panic("implement me")
}

func (g *FakeGraveler) ListTags(ctx context.Context, repository *graveler.RepositoryRecord) (graveler.TagIterator, error) {
	if g.Err != nil {
		return nil, g.Err
	}
	return g.TagIteratorFactory(), nil
}

func (g *FakeGraveler) Log(ctx context.Context, repository *graveler.RepositoryRecord, commitID graveler.CommitID, firstParent bool, since *time.Time) (graveler.CommitIterator, error) {
	panic("implement me")
}

func (g *FakeGraveler) ListBranches(ctx context.Context, repository *graveler.RepositoryRecord, opts ...graveler.ListOptionsFunc) (graveler.BranchIterator, error) {
	if g.Err != nil {
		return nil, g.Err
	}
	return g.BranchIteratorFactory(), nil
}

func (g *FakeGraveler) DeleteBranch(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, _ ...graveler.SetOptionsFunc) error {
	panic("implement me")
}

func (g *FakeGraveler) Commit(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, _ graveler.CommitParams, _ ...graveler.SetOptionsFunc) (graveler.CommitID, error) {
	panic("implement me")
}

func (g *FakeGraveler) CreateCommitRecord(ctx context.Context, repository *graveler.RepositoryRecord, commitID graveler.CommitID, commit graveler.Commit, opts ...graveler.SetOptionsFunc) error {
	panic("implement me")
}

func (g *FakeGraveler) WriteRange(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.ValueIterator, _ ...graveler.SetOptionsFunc) (*graveler.RangeInfo, error) {
	panic("implement me")
}

func (g *FakeGraveler) WriteMetaRange(ctx context.Context, repository *graveler.RepositoryRecord, ranges []*graveler.RangeInfo, _ ...graveler.SetOptionsFunc) (*graveler.MetaRangeInfo, error) {
	panic("implement me")
}

func (g *FakeGraveler) GetCommit(ctx context.Context, repository *graveler.RepositoryRecord, commitID graveler.CommitID) (*graveler.Commit, error) {
	panic("implement me")
}

func (g *FakeGraveler) Dereference(ctx context.Context, repository *graveler.RepositoryRecord, ref graveler.Ref) (*graveler.ResolvedRef, error) {
	panic("implement me")
}

func (g *FakeGraveler) Reset(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, _ ...graveler.SetOptionsFunc) error {
	panic("implement me")
}

func (g *FakeGraveler) ResetKey(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, key graveler.Key, _ ...graveler.SetOptionsFunc) error {
	panic("implement me")
}

func (g *FakeGraveler) ResetPrefix(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, key graveler.Key, _ ...graveler.SetOptionsFunc) error {
	panic("implement me")
}

func (g *FakeGraveler) Revert(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, _ graveler.Ref, _ int, _ graveler.CommitParams, _ *graveler.CommitOverrides, _ ...graveler.SetOptionsFunc) (graveler.CommitID, error) {
	panic("implement me")
}

func (g *FakeGraveler) Merge(ctx context.Context, repository *graveler.RepositoryRecord, destination graveler.BranchID, source graveler.Ref, _ graveler.CommitParams, strategy string, _ ...graveler.SetOptionsFunc) (graveler.CommitID, error) {
	panic("implement me")
}

func (g *FakeGraveler) FindMergeBase(ctx context.Context, repository *graveler.RepositoryRecord, from graveler.Ref, to graveler.Ref) (*graveler.CommitRecord, *graveler.CommitRecord, *graveler.Commit, error) {
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

func (g *FakeGraveler) AddCommit(ctx context.Context, repository *graveler.RepositoryRecord, commit graveler.Commit, opts ...graveler.SetOptionsFunc) (graveler.CommitID, error) {
	panic("implement me")
}

func (g *FakeGraveler) AddCommitNoLock(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.Commit) (graveler.CommitID, error) {
	panic("implement me")
}

func (g *FakeGraveler) WriteMetaRangeByIterator(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.ValueIterator, opts ...graveler.SetOptionsFunc) (*graveler.MetaRangeID, error) {
	panic("implement me")
}

func (g *FakeGraveler) GetStagingToken(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID) (*graveler.StagingToken, error) {
	panic("implement me")
}

func (g *FakeGraveler) GetPullRequest(context.Context, *graveler.RepositoryRecord, graveler.PullRequestID) (*graveler.PullRequest, error) {
	panic("implement me")
}

func (g *FakeGraveler) CreatePullRequest(context.Context, *graveler.RepositoryRecord, *graveler.PullRequestRecord) error {
	panic("implement me")
}

func (g *FakeGraveler) ListPullRequests(context.Context, *graveler.RepositoryRecord) (graveler.PullsIterator, error) {
	panic("implement me")
}

func (g *FakeGraveler) UpdatePullRequest(context.Context, *graveler.RepositoryRecord, graveler.PullRequestID, *graveler.UpdatePullRequest) error {
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

func NewFakeStagingIteratorFactory(data map[graveler.StagingToken][]*graveler.ValueRecord) func(token graveler.StagingToken) graveler.ValueIterator {
	return func(token graveler.StagingToken) graveler.ValueIterator {
		v, ok := data[token]
		if !ok {
			return NewFakeValueIterator([]*graveler.ValueRecord{})
		}
		return NewFakeValueIterator(v)
	}
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
