package testutil

import (
	"bytes"
	"context"
	"fmt"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed/tree"
)

const DefaultBranchID = graveler.BranchID("master")

type AppliedData struct {
	Values  graveler.ValueIterator
	RangeID graveler.RangeID
}

type CommittedFake struct {
	Value         *graveler.Value
	ValueIterator graveler.ValueIterator
	diffIterator  graveler.DiffIterator
	Err           error
	RangeID       graveler.RangeID
	AppliedData   AppliedData
}

type TreeFake struct {
	id graveler.RangeID
}

func (t *TreeFake) ID() graveler.RangeID {
	return t.id
}

func NewCommittedFake() graveler.CommittedManager {
	return &CommittedFake{}
}

func (c *CommittedFake) Get(_ context.Context, _ graveler.StorageNamespace, _ graveler.RangeID, _ graveler.Key) (*graveler.Value, error) {
	if c.Err != nil {
		return nil, c.Err
	}
	return c.Value, nil
}

func (c *CommittedFake) GetMetaRange(_ graveler.StorageNamespace, rangeID graveler.RangeID) (graveler.MetaRange, error) {
	if c.Err != nil {
		return nil, c.Err
	}
	return &TreeFake{id: rangeID}, nil
}

func (c *CommittedFake) List(_ context.Context, _ graveler.StorageNamespace, _ graveler.RangeID) (graveler.ValueIterator, error) {
	if c.Err != nil {
		return nil, c.Err
	}
	return c.ValueIterator, nil
}

func (c *CommittedFake) Diff(_ context.Context, _ graveler.StorageNamespace, _, _ graveler.RangeID) (graveler.DiffIterator, error) {
	if c.Err != nil {
		return nil, c.Err
	}
	return c.diffIterator, nil
}

func (c *CommittedFake) Merge(_ context.Context, _ graveler.StorageNamespace, _, _, _ graveler.RangeID, _, _ string, _ graveler.Metadata) (graveler.RangeID, error) {
	if c.Err != nil {
		return "", c.Err
	}
	return c.RangeID, nil
}

func (c *CommittedFake) Apply(_ context.Context, _ graveler.StorageNamespace, rangeID graveler.RangeID, values graveler.ValueIterator) (graveler.RangeID, error) {
	if c.Err != nil {
		return "", c.Err
	}
	c.AppliedData.Values = values
	c.AppliedData.RangeID = rangeID
	return c.RangeID, nil
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

func (s *StagingFake) DropByPrefix(_ context.Context, _ graveler.StagingToken, _ graveler.Key) error {
	return nil
}

func (s *StagingFake) Drop(_ context.Context, _ graveler.StagingToken) error {
	s.DropCalled = true
	if s.DropErr != nil {
		return s.DropErr
	}
	return nil
}

func (s *StagingFake) Get(_ context.Context, _ graveler.StagingToken, _ graveler.Key) (*graveler.Value, error) {
	if s.Err != nil {
		return nil, s.Err
	}
	return s.Value, nil
}

func (s *StagingFake) Set(_ context.Context, _ graveler.StagingToken, key graveler.Key, value *graveler.Value) error {
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

func (s *StagingFake) List(_ context.Context, _ graveler.StagingToken) (graveler.ValueIterator, error) {
	if s.Err != nil {
		return nil, s.Err
	}
	return s.ValueIterator, nil
}

func (s *StagingFake) Snapshot(_ context.Context, _ graveler.StagingToken) (graveler.StagingToken, error) {
	if s.Err != nil {
		return "", s.Err
	}
	return s.stagingToken, nil
}

func (s *StagingFake) ListSnapshot(_ context.Context, _ graveler.StagingToken, _ graveler.Key) (graveler.ValueIterator, error) {
	if s.Err != nil {
		return nil, s.Err
	}
	return s.ValueIterator, nil
}

type AddedCommitData struct {
	Committer string
	Message   string
	RangeID   graveler.RangeID
	Parents   graveler.CommitParents
	Metadata  graveler.Metadata
}

type RefsFake struct {
	ListRepositoriesRes graveler.RepositoryIterator
	ListBranchesRes     graveler.BranchIterator
	ListTagsRes         graveler.TagIterator
	CommitIter          graveler.CommitIterator
	RefType             graveler.ReferenceType
	Branch              *graveler.Branch
	TagCommitID         *graveler.CommitID
	Err                 error
	CommitErr           error
	AddedCommit         AddedCommitData
	CommitID            graveler.CommitID
	Commit              *graveler.Commit
}

func (m *RefsFake) RevParse(_ context.Context, _ graveler.RepositoryID, _ graveler.Ref) (graveler.Reference, error) {
	var branch graveler.BranchID
	if m.RefType == graveler.ReferenceTypeBranch {
		branch = DefaultBranchID
	}
	return NewFakeReference(m.RefType, branch, ""), nil
}

func (m *RefsFake) GetRepository(_ context.Context, _ graveler.RepositoryID) (*graveler.Repository, error) {
	return &graveler.Repository{}, nil
}

func (m *RefsFake) CreateRepository(_ context.Context, _ graveler.RepositoryID, _ graveler.Repository, _ graveler.Branch) error {
	return nil
}

func (m *RefsFake) ListRepositories(_ context.Context) (graveler.RepositoryIterator, error) {
	return m.ListRepositoriesRes, nil
}

func (m *RefsFake) DeleteRepository(_ context.Context, _ graveler.RepositoryID) error {
	return nil
}

func (m *RefsFake) GetBranch(_ context.Context, _ graveler.RepositoryID, _ graveler.BranchID) (*graveler.Branch, error) {
	return m.Branch, m.Err
}

func (m *RefsFake) SetBranch(_ context.Context, _ graveler.RepositoryID, _ graveler.BranchID, _ graveler.Branch) error {
	return nil
}

func (m *RefsFake) DeleteBranch(_ context.Context, _ graveler.RepositoryID, _ graveler.BranchID) error {
	return nil
}

func (m *RefsFake) ListBranches(_ context.Context, _ graveler.RepositoryID) (graveler.BranchIterator, error) {
	return m.ListBranchesRes, nil
}

func (m *RefsFake) GetTag(_ context.Context, _ graveler.RepositoryID, _ graveler.TagID) (*graveler.CommitID, error) {
	return m.TagCommitID, m.Err
}

func (m *RefsFake) CreateTag(_ context.Context, _ graveler.RepositoryID, _ graveler.TagID, _ graveler.CommitID) error {
	return nil
}

func (m *RefsFake) DeleteTag(_ context.Context, _ graveler.RepositoryID, _ graveler.TagID) error {
	return nil
}

func (m *RefsFake) ListTags(_ context.Context, _ graveler.RepositoryID) (graveler.TagIterator, error) {
	return m.ListTagsRes, nil
}

func (m *RefsFake) GetCommit(_ context.Context, _ graveler.RepositoryID, _ graveler.CommitID) (*graveler.Commit, error) {
	return m.Commit, nil
}

func (m *RefsFake) AddCommit(_ context.Context, _ graveler.RepositoryID, commit graveler.Commit) (graveler.CommitID, error) {
	if m.CommitErr != nil {
		return "", m.CommitErr
	}
	m.AddedCommit = AddedCommitData{
		Committer: commit.Committer,
		Message:   commit.Message,
		RangeID:   commit.RangeID,
		Parents:   commit.Parents,
		Metadata:  commit.Metadata,
	}
	return m.CommitID, nil
}

func (m *RefsFake) FindMergeBase(_ context.Context, _ graveler.RepositoryID, _ ...graveler.CommitID) (*graveler.Commit, error) {
	return &graveler.Commit{}, nil
}

func (m *RefsFake) Log(_ context.Context, _ graveler.RepositoryID, _ graveler.CommitID) (graveler.CommitIterator, error) {
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
	for i, record := range r.records {
		if bytes.Compare(id, record.Key) >= 0 {
			r.current = i - 1
		}
	}
	r.current = len(r.records)
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

type referenceFake struct {
	refType  graveler.ReferenceType
	branch   graveler.Branch
	commitID graveler.CommitID
}

// NewFakeReference returns a referenceFake
// if branch parameter is empty branch record will be nil
func NewFakeReference(refType graveler.ReferenceType, branchID graveler.BranchID, commitID graveler.CommitID) *referenceFake {
	var branch graveler.Branch
	if branchID != "" {
		branch = graveler.Branch{CommitID: commitID}
	}
	return &referenceFake{
		refType:  refType,
		branch:   branch,
		commitID: commitID,
	}
}

func (m *referenceFake) Type() graveler.ReferenceType {
	return m.refType
}

func (m *referenceFake) Branch() graveler.Branch {
	return m.branch
}

func (m *referenceFake) CommitID() graveler.CommitID {
	return m.commitID
}

type PV struct {
	P *tree.Part
	V *graveler.ValueRecord
}

type FakePartsAndValuesIterator struct {
	PV          []PV
	idx         int
	partIdx     int
	err         error
	closed      bool
	readsByPart []int
}

// ReadsByPart returns the number of Next operations performed inside each part
func (i *FakePartsAndValuesIterator) ReadsByPart() []int {
	return i.readsByPart
}

func (i *FakePartsAndValuesIterator) nextKey() []byte {
	if len(i.PV) <= i.idx+1 {
		return nil
	}
	if i.PV[i.idx+1].V == nil {
		return i.PV[i.idx+1].P.MinKey
	}
	return i.PV[i.idx+1].V.Key
}

func NewFakePartsAndValuesIterator() *FakePartsAndValuesIterator {
	// Start with an empty record so the first `Next()` can skip it.
	return &FakePartsAndValuesIterator{PV: make([]PV, 1), idx: 0, partIdx: -1}
}

func (i *FakePartsAndValuesIterator) SetErr(err error) {
	i.err = err
}

func (i *FakePartsAndValuesIterator) AddPart(p *tree.Part) *FakePartsAndValuesIterator {
	i.PV = append(i.PV, PV{P: p})
	i.readsByPart = append(i.readsByPart, 0)
	return i
}

func (i *FakePartsAndValuesIterator) AddValueRecords(vs ...*graveler.ValueRecord) *FakePartsAndValuesIterator {
	if len(i.PV) == 0 {
		panic(fmt.Sprintf("cannot add ValueRecords %+v with no part", vs))
	}
	part := i.PV[len(i.PV)-1].P
	for _, v := range vs {
		i.PV = append(i.PV, PV{P: part, V: v})
	}
	return i
}

func (i *FakePartsAndValuesIterator) Next() bool {
	if i.err != nil || i.closed {
		return false
	}
	if len(i.PV) <= i.idx+1 {
		return false
	}
	i.idx++
	if i.PV[i.idx].V == nil {
		i.partIdx++
	} else {
		i.readsByPart[i.partIdx]++
	}
	return true
}

func (i *FakePartsAndValuesIterator) NextPart() bool {
	for {
		if len(i.PV) <= i.idx+1 {
			return false
		}
		i.idx++
		if i.PV[i.idx].V == nil {
			i.partIdx++
			return true
		}
	}
}

func (i *FakePartsAndValuesIterator) Value() (*graveler.ValueRecord, *tree.Part) {
	if i.closed {
		return nil, nil
	}
	return i.PV[i.idx].V, i.PV[i.idx].P
}

func (i *FakePartsAndValuesIterator) SeekGE(id graveler.Key) {
	i.idx = 0
	i.partIdx = -1
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

func (i *FakePartsAndValuesIterator) Err() error {
	return i.err
}

func (i *FakePartsAndValuesIterator) Close() {
	i.closed = true
}
