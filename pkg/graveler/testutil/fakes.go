package testutil

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
)

const DefaultBranchID = graveler.BranchID("main")

type AppliedData struct {
	Values      graveler.ValueIterator
	MetaRangeID graveler.MetaRangeID
}

type CommittedFake struct {
	ValuesByKey   map[string]*graveler.Value
	ValueIterator graveler.ValueIterator
	DiffIterator  graveler.DiffIterator
	Err           error
	MetaRangeID   graveler.MetaRangeID
	DiffSummary   graveler.DiffSummary
	AppliedData   AppliedData
}

type MetaRangeFake struct {
	id graveler.MetaRangeID
}

func (t *MetaRangeFake) ID() graveler.MetaRangeID {
	return t.id
}

func (c *CommittedFake) Exists(context.Context, graveler.StorageNamespace, graveler.MetaRangeID) (bool, error) {
	if c.Err != nil {
		return false, c.Err
	}
	return true, nil
}

func (c *CommittedFake) Get(_ context.Context, _ graveler.StorageNamespace, _ graveler.MetaRangeID, key graveler.Key) (*graveler.Value, error) {
	if c.Err != nil {
		return nil, c.Err
	}
	return c.ValuesByKey[string(key)], nil
}

func (c *CommittedFake) List(context.Context, graveler.StorageNamespace, graveler.MetaRangeID) (graveler.ValueIterator, error) {
	if c.Err != nil {
		return nil, c.Err
	}
	return c.ValueIterator, nil
}

func (c *CommittedFake) Diff(context.Context, graveler.StorageNamespace, graveler.MetaRangeID, graveler.MetaRangeID) (graveler.DiffIterator, error) {
	if c.Err != nil {
		return nil, c.Err
	}
	return c.DiffIterator, nil
}

func (c *CommittedFake) Compare(context.Context, graveler.StorageNamespace, graveler.MetaRangeID, graveler.MetaRangeID, graveler.MetaRangeID) (graveler.DiffIterator, error) {
	if c.Err != nil {
		return nil, c.Err
	}
	return c.DiffIterator, nil
}

func (c *CommittedFake) Merge(_ context.Context, _ graveler.StorageNamespace, _, _, _ graveler.MetaRangeID) (graveler.MetaRangeID, graveler.DiffSummary, error) {
	if c.Err != nil {
		return "", graveler.DiffSummary{}, c.Err
	}
	return c.MetaRangeID, c.DiffSummary, nil
}

func (c *CommittedFake) Apply(_ context.Context, _ graveler.StorageNamespace, metaRangeID graveler.MetaRangeID, values graveler.ValueIterator) (graveler.MetaRangeID, graveler.DiffSummary, error) {
	if c.Err != nil {
		return "", graveler.DiffSummary{}, c.Err
	}
	c.AppliedData.Values = values
	c.AppliedData.MetaRangeID = metaRangeID
	return c.MetaRangeID, c.DiffSummary, nil
}

func (c *CommittedFake) WriteMetaRange(ctx context.Context, ns graveler.StorageNamespace, it graveler.ValueIterator, metadata graveler.Metadata) (*graveler.MetaRangeID, error) {
	if c.Err != nil {
		return nil, c.Err
	}
	return &c.MetaRangeID, nil
}

func (c *CommittedFake) GetMetaRange(ctx context.Context, ns graveler.StorageNamespace, metaRangeID graveler.MetaRangeID) (graveler.MetaRangeInfo, error) {
	return graveler.MetaRangeInfo{
		Address: fmt.Sprintf("fake://prefix/%s(metarange)", metaRangeID),
	}, nil
}

func (c *CommittedFake) GetRange(ctx context.Context, ns graveler.StorageNamespace, rangeID graveler.RangeID) (graveler.RangeInfo, error) {
	return graveler.RangeInfo{
		Address: fmt.Sprintf("fake://prefix/%s(range)", rangeID),
	}, nil
}

type StagingFake struct {
	Err                error
	DropErr            error // specific error for drop call
	Value              *graveler.Value
	ValueIterator      graveler.ValueIterator
	stagingToken       graveler.StagingToken
	LastSetValueRecord *graveler.ValueRecord
	LastRemovedKey     graveler.Key
	DropCalled         bool
	SetErr             error
}

func (s *StagingFake) DropByPrefix(context.Context, graveler.StagingToken, graveler.Key) error {
	return nil
}

func (s *StagingFake) Drop(context.Context, graveler.StagingToken) error {
	s.DropCalled = true
	if s.DropErr != nil {
		return s.DropErr
	}
	return nil
}

func (s *StagingFake) Get(context.Context, graveler.StagingToken, graveler.Key) (*graveler.Value, error) {
	if s.Err != nil {
		return nil, s.Err
	}
	return s.Value, nil
}

func (s *StagingFake) Set(_ context.Context, _ graveler.StagingToken, key graveler.Key, value *graveler.Value, _ bool) error {
	if s.SetErr != nil {
		return s.SetErr
	}
	s.LastSetValueRecord = &graveler.ValueRecord{
		Key:   key,
		Value: value,
	}
	return nil
}

func (s *StagingFake) DropKey(_ context.Context, _ graveler.StagingToken, key graveler.Key) error {
	if s.Err != nil {
		return s.Err
	}
	s.LastRemovedKey = key
	return nil
}

func (s *StagingFake) List(context.Context, graveler.StagingToken) (graveler.ValueIterator, error) {
	if s.Err != nil {
		return nil, s.Err
	}
	return s.ValueIterator, nil
}

func (s *StagingFake) Snapshot(context.Context, graveler.StagingToken) (graveler.StagingToken, error) {
	if s.Err != nil {
		return "", s.Err
	}
	return s.stagingToken, nil
}

func (s *StagingFake) ListSnapshot(context.Context, graveler.StagingToken, graveler.Key) (graveler.ValueIterator, error) {
	if s.Err != nil {
		return nil, s.Err
	}
	return s.ValueIterator, nil
}

type AddedCommitData struct {
	Committer   string
	Message     string
	MetaRangeID graveler.MetaRangeID
	Parents     graveler.CommitParents
	Metadata    graveler.Metadata
}

type RefsFake struct {
	ListRepositoriesRes graveler.RepositoryIterator
	ListBranchesRes     graveler.BranchIterator
	Refs                map[graveler.Ref]*graveler.ResolvedRef
	ListTagsRes         graveler.TagIterator
	CommitIter          graveler.CommitIterator
	RefType             graveler.ReferenceType
	Branch              *graveler.Branch
	TagCommitID         *graveler.CommitID
	Err                 error
	CommitErr           error
	AddedCommit         AddedCommitData
	CommitID            graveler.CommitID
	Commits             map[graveler.CommitID]*graveler.Commit
	StagingToken        graveler.StagingToken
}

func (m *RefsFake) FillGenerations(ctx context.Context, repositoryID graveler.RepositoryID) error {
	panic("implement me")
}

func (m *RefsFake) CreateBareRepository(ctx context.Context, repositoryID graveler.RepositoryID, repository graveler.Repository) error {
	panic("implement me")
}

func (m *RefsFake) ListCommits(ctx context.Context, repositoryID graveler.RepositoryID) (graveler.CommitIterator, error) {
	return nil, nil
}

func (m *RefsFake) ParseRef(ref graveler.Ref) (graveler.RawRef, error) {
	// fake so we use the base ref to capture the ref for resolve lookup
	return graveler.RawRef{
		BaseRef: string(ref),
	}, nil
}

func (m *RefsFake) ResolveRawRef(ctx context.Context, repoID graveler.RepositoryID, rawRef graveler.RawRef) (*graveler.ResolvedRef, error) {
	if m.Refs != nil {
		ref := graveler.Ref(rawRef.BaseRef)
		if res, ok := m.Refs[ref]; ok {
			return res, nil
		}
	}

	var branch graveler.BranchID
	var stagingToken graveler.StagingToken
	if m.RefType == graveler.ReferenceTypeBranch {
		branch = DefaultBranchID
		stagingToken = m.StagingToken
	}

	return &graveler.ResolvedRef{
		Type:         m.RefType,
		BranchID:     branch,
		CommitID:     m.CommitID,
		StagingToken: stagingToken,
	}, nil
}

func (m *RefsFake) GetRepository(context.Context, graveler.RepositoryID) (*graveler.Repository, error) {
	return &graveler.Repository{}, nil
}

func (m *RefsFake) CreateRepository(context.Context, graveler.RepositoryID, graveler.Repository, graveler.StagingToken) error {
	return nil
}

func (m *RefsFake) ListRepositories(context.Context) (graveler.RepositoryIterator, error) {
	return m.ListRepositoriesRes, nil
}

func (m *RefsFake) DeleteRepository(context.Context, graveler.RepositoryID) error {
	return nil
}

func (m *RefsFake) GetBranch(context.Context, graveler.RepositoryID, graveler.BranchID) (*graveler.Branch, error) {
	return m.Branch, m.Err
}

func (m *RefsFake) SetBranch(context.Context, graveler.RepositoryID, graveler.BranchID, graveler.Branch) error {
	return nil
}

func (m *RefsFake) DeleteBranch(context.Context, graveler.RepositoryID, graveler.BranchID) error {
	return nil
}

func (m *RefsFake) ListBranches(context.Context, graveler.RepositoryID) (graveler.BranchIterator, error) {
	return m.ListBranchesRes, nil
}

func (m *RefsFake) GetTag(context.Context, graveler.RepositoryID, graveler.TagID) (*graveler.CommitID, error) {
	return m.TagCommitID, m.Err
}

func (m *RefsFake) CreateTag(context.Context, graveler.RepositoryID, graveler.TagID, graveler.CommitID) error {
	return nil
}

func (m *RefsFake) DeleteTag(context.Context, graveler.RepositoryID, graveler.TagID) error {
	return nil
}

func (m *RefsFake) ListTags(context.Context, graveler.RepositoryID) (graveler.TagIterator, error) {
	return m.ListTagsRes, nil
}

func (m *RefsFake) GetCommit(_ context.Context, _ graveler.RepositoryID, id graveler.CommitID) (*graveler.Commit, error) {
	if val, ok := m.Commits[id]; ok {
		return val, nil
	}

	return nil, graveler.ErrCommitNotFound
}

func (m *RefsFake) AddCommit(_ context.Context, _ graveler.RepositoryID, commit graveler.Commit) (graveler.CommitID, error) {
	if m.CommitErr != nil {
		return "", m.CommitErr
	}
	m.AddedCommit = AddedCommitData{
		Committer:   commit.Committer,
		Message:     commit.Message,
		MetaRangeID: commit.MetaRangeID,
		Parents:     commit.Parents,
		Metadata:    commit.Metadata,
	}
	return m.CommitID, nil
}

func (m *RefsFake) FindMergeBase(context.Context, graveler.RepositoryID, ...graveler.CommitID) (*graveler.Commit, error) {
	return &graveler.Commit{}, nil
}

func (m *RefsFake) Log(context.Context, graveler.RepositoryID, graveler.CommitID) (graveler.CommitIterator, error) {
	return m.CommitIter, nil
}

type diffIter struct {
	current int
	records []graveler.Diff
	err     error
}

func NewDiffIter(records []graveler.Diff) *diffIter {
	return &diffIter{records: records, current: -1}
}
func (r *diffIter) Next() bool {
	r.current++
	return r.current < len(r.records)
}

func (r *diffIter) SeekGE(id graveler.Key) {
	i := sort.Search(len(r.records), func(i int) bool {
		return bytes.Compare(r.records[i].Key, id) >= 0
	})
	r.current = i - 1
}

func (r *diffIter) Value() *graveler.Diff {
	if r.current < 0 || r.current >= len(r.records) {
		return nil
	}
	return &r.records[r.current]
}

func (r *diffIter) Err() error {
	return r.err
}

func (r *diffIter) Close() {}

type valueIteratorFake struct {
	current int
	records []graveler.ValueRecord
	err     error
}

func NewValueIteratorFake(records []graveler.ValueRecord) graveler.ValueIterator {
	return &valueIteratorFake{records: records, current: -1}
}

func (r *valueIteratorFake) Next() bool {
	r.current++
	return r.current < len(r.records)
}

func (r *valueIteratorFake) SeekGE(id graveler.Key) {
	for i, record := range r.records {
		if bytes.Compare(record.Key, id) >= 0 {
			r.current = i - 1
			return
		}
	}
	r.current = len(r.records)
}

func (r *valueIteratorFake) Value() *graveler.ValueRecord {
	if r.current < 0 || r.current >= len(r.records) {
		return nil
	}
	return &r.records[r.current]
}

func (r *valueIteratorFake) Err() error {
	return r.err
}

func (r *valueIteratorFake) Close() {}

type committedValueIteratorFake struct {
	current int
	records []committed.Record
	err     error
}

func NewCommittedValueIteratorFake(records []committed.Record) *committedValueIteratorFake {
	return &committedValueIteratorFake{records: records, current: -1}
}

func (r *committedValueIteratorFake) Next() bool {
	r.current++
	return r.current < len(r.records)
}

func (r *committedValueIteratorFake) SeekGE(id committed.Key) {
	i := sort.Search(len(r.records), func(i int) bool {
		return bytes.Compare(r.records[i].Key, id) >= 0
	})
	r.current = i - 1
}

func (r *committedValueIteratorFake) Value() *committed.Record {
	if r.current < 0 || r.current >= len(r.records) {
		return nil
	}
	return &r.records[r.current]
}

func (r *committedValueIteratorFake) Err() error {
	return r.err
}

func (r *committedValueIteratorFake) Close() {}

type RV struct {
	R *committed.Range
	V *graveler.ValueRecord
}

type FakeIterator struct {
	RV           []RV
	idx          int
	rangeIdx     int
	err          error
	closed       bool
	readsByRange []int
}

func NewFakeIterator() *FakeIterator {
	// Start with an empty record so the first `Next()` can skip it.
	return &FakeIterator{RV: make([]RV, 1), idx: 0, rangeIdx: -1}
}

// ReadsByRange returns the number of Next operations performed inside each range
func (i *FakeIterator) ReadsByRange() []int {
	return i.readsByRange
}

func (i *FakeIterator) nextKey() []byte {
	if len(i.RV) <= i.idx+1 {
		return nil
	}
	if i.RV[i.idx+1].V == nil {
		return i.RV[i.idx+1].R.MinKey
	}
	return i.RV[i.idx+1].V.Key
}

func (i *FakeIterator) SetErr(err error) {
	i.err = err
}

func (i *FakeIterator) AddRange(p *committed.Range) *FakeIterator {
	i.RV = append(i.RV, RV{R: p})
	i.readsByRange = append(i.readsByRange, 0)
	return i
}

func (i *FakeIterator) AddValueRecords(vs ...*graveler.ValueRecord) *FakeIterator {
	if len(i.RV) == 0 {
		panic(fmt.Sprintf("cannot add ValueRecords %+v with no range", vs))
	}
	rng := i.RV[len(i.RV)-1].R
	for _, v := range vs {
		i.RV = append(i.RV, RV{R: rng, V: v})
	}
	return i
}

func (i *FakeIterator) Next() bool {
	if i.err != nil || i.closed {
		return false
	}
	if len(i.RV) <= i.idx+1 {
		return false
	}
	i.idx++
	if i.RV[i.idx].V == nil {
		i.rangeIdx++
	} else {
		i.readsByRange[i.rangeIdx]++
	}
	return true
}

func (i *FakeIterator) NextRange() bool {
	for {
		if len(i.RV) <= i.idx+1 {
			return false
		}
		i.idx++
		if i.RV[i.idx].V == nil {
			i.rangeIdx++
			return true
		}
	}
}

func (i *FakeIterator) Value() (*graveler.ValueRecord, *committed.Range) {
	if i.closed {
		return nil, nil
	}
	return i.RV[i.idx].V, i.RV[i.idx].R
}

func (i *FakeIterator) SeekGE(id graveler.Key) {
	i.idx = 0
	i.rangeIdx = -1
	for {
		nextKey := i.nextKey()
		if nextKey == nil || bytes.Compare(nextKey, id) >= 0 {
			return
		}
		if !i.Next() {
			return
		}
	}
}

func (i *FakeIterator) Err() error {
	return i.err
}

func (i *FakeIterator) Close() {
	i.closed = true
}

type DRV struct {
	R *committed.RangeDiff
	V *graveler.Diff
}

type FakeDiffIterator struct {
	DRV          []DRV
	idx          int
	rangeIdx     int
	err          error
	closed       bool
	readsByRange []int
}

func NewFakeDiffIterator() *FakeDiffIterator {
	// Start with an empty record so the first `Next()` can skip it.
	return &FakeDiffIterator{DRV: make([]DRV, 1), idx: 0, rangeIdx: -1}
}

// ReadsByRange returns the number of Next operations performed inside each range
func (i *FakeDiffIterator) ReadsByRange() []int {
	return i.readsByRange
}

func (i *FakeDiffIterator) nextKey() []byte {
	for j := 1; len(i.DRV) > i.idx+j; j++ {
		if i.DRV[i.idx+j].V == nil {
			if i.DRV[i.idx+j].R == nil {
				continue // in case we added an empty range header
			}
			return i.DRV[i.idx+j].R.Range.MinKey
		}
		return i.DRV[i.idx+j].V.Key
	}
	return nil
}

func (i *FakeDiffIterator) SetErr(err error) {
	i.err = err
}

func (i *FakeDiffIterator) AddRange(p *committed.RangeDiff) *FakeDiffIterator {
	i.DRV = append(i.DRV, DRV{R: p})
	i.readsByRange = append(i.readsByRange, 0)
	return i
}

func (i *FakeDiffIterator) AddValueRecords(vs ...*graveler.Diff) *FakeDiffIterator {
	if len(i.DRV) == 0 {
		panic(fmt.Sprintf("cannot add ValueRecords %+v with no range", vs))
	}
	rng := i.DRV[len(i.DRV)-1].R
	for _, v := range vs {
		i.DRV = append(i.DRV, DRV{R: rng, V: v})
	}
	return i
}

func (i *FakeDiffIterator) Next() bool {
	if i.err != nil || i.closed {
		return false
	}
	if len(i.DRV) <= i.idx+1 {
		return false
	}
	i.idx++
	if i.DRV[i.idx].V == nil {
		i.rangeIdx++
		if i.DRV[i.idx].R == nil {
			return i.Next() // in case we added an empty range header
		}
	} else if i.DRV[i.idx].R != nil {
		i.readsByRange[i.rangeIdx]++
	}
	return true
}

func (i *FakeDiffIterator) NextRange() bool {
	for {
		if len(i.DRV) <= i.idx+1 {
			return false
		}
		i.idx++
		if i.DRV[i.idx].V == nil {
			i.rangeIdx++
			if i.DRV[i.idx].R == nil {
				return i.Next() // in case we added an empty range header
			}
			return true
		}
	}
}

func (i *FakeDiffIterator) Value() (*graveler.Diff, *committed.RangeDiff) {
	if i.closed {
		return nil, nil
	}
	return i.DRV[i.idx].V, i.DRV[i.idx].R
}

func (i *FakeDiffIterator) SeekGE(id graveler.Key) {
	i.idx = 0
	i.rangeIdx = -1
	for {
		nextKey := i.nextKey()
		if nextKey == nil || bytes.Compare(nextKey, id) >= 0 {
			return
		}
		if !i.Next() {
			return
		}
	}
}

func (i *FakeDiffIterator) Err() error {
	return i.err
}

func (i *FakeDiffIterator) Close() {
	i.closed = true
}

type FakeBranchIterator struct {
	Data  []*graveler.BranchRecord
	Index int
}

func NewFakeBranchIterator(data []*graveler.BranchRecord) *FakeBranchIterator {
	return &FakeBranchIterator{Data: data, Index: -1}
}

func NewFakeBranchIteratorFactory(data []*graveler.BranchRecord) func() graveler.BranchIterator {
	return func() graveler.BranchIterator { return NewFakeBranchIterator(data) }
}

func (m *FakeBranchIterator) Next() bool {
	if m.Index >= len(m.Data) {
		return false
	}
	m.Index++
	return m.Index < len(m.Data)
}

func (m *FakeBranchIterator) SeekGE(id graveler.BranchID) {
	m.Index = len(m.Data)
	for i, item := range m.Data {
		if item.BranchID >= id {
			m.Index = i - 1
			return
		}
	}
}

func (m *FakeBranchIterator) Value() *graveler.BranchRecord {
	return m.Data[m.Index]
}

func (m *FakeBranchIterator) Err() error {
	return nil
}

func (m *FakeBranchIterator) Close() {}
