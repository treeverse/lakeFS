package graveler_test

import (
	"bytes"
	"context"
	"errors"
	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/graveler/testutil"
	tu "github.com/treeverse/lakefs/pkg/testutil"
	"strconv"
	"testing"
)

type Hooks struct {
	Called           bool
	Err              error
	RunID            string
	RepositoryID     graveler.RepositoryID
	StorageNamespace graveler.StorageNamespace
	BranchID         graveler.BranchID
	SourceRef        graveler.Ref
	CommitID         graveler.CommitID
	Commit           graveler.Commit
	TagID            graveler.TagID
}

func (h *Hooks) PreCommitHook(_ context.Context, record graveler.HookRecord) error {
	h.Called = true
	h.RepositoryID = record.RepositoryID
	h.StorageNamespace = record.StorageNamespace
	h.BranchID = record.BranchID
	h.Commit = record.Commit
	return h.Err
}

func (h *Hooks) PostCommitHook(_ context.Context, record graveler.HookRecord) error {
	h.Called = true
	h.RepositoryID = record.RepositoryID
	h.BranchID = record.BranchID
	h.CommitID = record.CommitID
	h.Commit = record.Commit
	return h.Err
}

func (h *Hooks) PreMergeHook(_ context.Context, record graveler.HookRecord) error {
	h.Called = true
	h.RepositoryID = record.RepositoryID
	h.StorageNamespace = record.StorageNamespace
	h.BranchID = record.BranchID
	h.SourceRef = record.SourceRef
	h.Commit = record.Commit
	return h.Err
}

func (h *Hooks) PostMergeHook(_ context.Context, record graveler.HookRecord) error {
	h.Called = true
	h.RepositoryID = record.RepositoryID
	h.StorageNamespace = record.StorageNamespace
	h.BranchID = record.BranchID
	h.SourceRef = record.SourceRef
	h.CommitID = record.CommitID
	h.Commit = record.Commit
	return h.Err
}

func (h *Hooks) PreCreateTagHook(_ context.Context, record graveler.HookRecord) error {
	h.Called = true
	h.StorageNamespace = record.StorageNamespace
	h.RepositoryID = record.RepositoryID
	h.CommitID = record.CommitID
	h.TagID = record.TagID
	return h.Err
}

func (h *Hooks) PostCreateTagHook(_ context.Context, record graveler.HookRecord) error {
	h.Called = true
	h.StorageNamespace = record.StorageNamespace
	h.RepositoryID = record.RepositoryID
	h.CommitID = record.CommitID
	h.TagID = record.TagID
	return h.Err
}

func (h *Hooks) PreDeleteTagHook(_ context.Context, record graveler.HookRecord) error {
	h.Called = true
	h.StorageNamespace = record.StorageNamespace
	h.RepositoryID = record.RepositoryID
	h.TagID = record.TagID
	return h.Err
}

func (h *Hooks) PostDeleteTagHook(_ context.Context, record graveler.HookRecord) error {
	h.Called = true
	h.StorageNamespace = record.StorageNamespace
	h.RepositoryID = record.RepositoryID
	h.TagID = record.TagID
	return h.Err
}

func (h *Hooks) PreCreateBranchHook(_ context.Context, record graveler.HookRecord) error {
	h.Called = true
	h.StorageNamespace = record.StorageNamespace
	h.RepositoryID = record.RepositoryID
	h.BranchID = record.BranchID
	h.CommitID = record.CommitID
	h.SourceRef = record.SourceRef
	return h.Err
}

func (h *Hooks) PostCreateBranchHook(_ context.Context, record graveler.HookRecord) error {
	h.Called = true
	h.StorageNamespace = record.StorageNamespace
	h.RepositoryID = record.RepositoryID
	h.BranchID = record.BranchID
	h.CommitID = record.CommitID
	h.SourceRef = record.SourceRef
	return h.Err
}

func (h *Hooks) PreDeleteBranchHook(_ context.Context, record graveler.HookRecord) error {
	h.Called = true
	h.StorageNamespace = record.StorageNamespace
	h.RepositoryID = record.RepositoryID
	h.BranchID = record.BranchID
	return h.Err
}

func (h *Hooks) PostDeleteBranchHook(_ context.Context, record graveler.HookRecord) error {
	h.Called = true
	h.StorageNamespace = record.StorageNamespace
	h.RepositoryID = record.RepositoryID
	h.BranchID = record.BranchID
	return h.Err
}

func TestGraveler_List(t *testing.T) {
	conn, _ := tu.GetDB(t, databaseURI)
	branchLocker := ref.NewBranchLocker(conn)
	tests := []struct {
		name        string
		r           *graveler.Graveler
		expectedErr error
		expected    []*graveler.ValueRecord
	}{
		{
			name: "one committed one staged no paths",
			r: graveler.NewGraveler(branchLocker, &testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo"), Value: &graveler.Value{}}})},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("bar"), Value: &graveler.Value{}}})},
				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, StagingToken: "token", Commits: map[graveler.CommitID]*graveler.Commit{"": {}}}, nil, testutil.NewProtectedBranchesManagerFake(),
			),
			expected: []*graveler.ValueRecord{{Key: graveler.Key("bar"), Value: &graveler.Value{}}, {Key: graveler.Key("foo"), Value: &graveler.Value{}}},
		},
		{
			name: "same path different file",
			r: graveler.NewGraveler(branchLocker, &testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo"), Value: &graveler.Value{Identity: []byte("original")}}})},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo"), Value: &graveler.Value{Identity: []byte("other")}}})},
				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, StagingToken: "token", Commits: map[graveler.CommitID]*graveler.Commit{"": {}}}, nil, testutil.NewProtectedBranchesManagerFake(),
			),
			expected: []*graveler.ValueRecord{{Key: graveler.Key("foo"), Value: &graveler.Value{Identity: []byte("other")}}},
		},
		{
			name: "one committed one staged no paths - with prefix",
			r: graveler.NewGraveler(branchLocker, &testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("prefix/foo"), Value: &graveler.Value{}}})},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("prefix/bar"), Value: &graveler.Value{}}})},
				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, StagingToken: "token", Commits: map[graveler.CommitID]*graveler.Commit{"": {}}}, nil, testutil.NewProtectedBranchesManagerFake(),
			),
			expected: []*graveler.ValueRecord{{Key: graveler.Key("prefix/bar"), Value: &graveler.Value{}}, {Key: graveler.Key("prefix/foo"), Value: &graveler.Value{}}},
		},
	}
	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listing, err := tt.r.List(ctx, "", "")
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("wrong error, expected:%s got:%s", tt.expectedErr, err)
			}
			if err != nil {
				return // err == tt.expectedErr
			}
			defer listing.Close()
			var recs []*graveler.ValueRecord
			for listing.Next() {
				recs = append(recs, listing.Value())
			}
			if diff := deep.Equal(recs, tt.expected); diff != nil {
				t.Fatal("List() diff found", diff)
			}
		})
	}
}

func TestGraveler_Get(t *testing.T) {
	conn, _ := tu.GetDB(t, databaseURI)
	branchLocker := ref.NewBranchLocker(conn)
	errTest := errors.New("some kind of err")
	tests := []struct {
		name                string
		r                   *graveler.Graveler
		expectedValueResult graveler.Value
		expectedErr         error
	}{
		{
			name: "commit - exists",
			r: graveler.NewGraveler(branchLocker, &testutil.CommittedFake{ValuesByKey: map[string]*graveler.Value{"key": {Identity: []byte("committed")}}}, nil,
				&testutil.RefsFake{RefType: graveler.ReferenceTypeCommit, Commits: map[graveler.CommitID]*graveler.Commit{"": {}}}, nil, testutil.NewProtectedBranchesManagerFake(),
			),
			expectedValueResult: graveler.Value{Identity: []byte("committed")},
		},
		{
			name: "commit - not found",
			r: graveler.NewGraveler(branchLocker, &testutil.CommittedFake{Err: graveler.ErrNotFound}, nil,
				&testutil.RefsFake{RefType: graveler.ReferenceTypeCommit, Commits: map[graveler.CommitID]*graveler.Commit{"": {}}}, nil, testutil.NewProtectedBranchesManagerFake(),
			), expectedErr: graveler.ErrNotFound,
		},
		{
			name: "commit - error",
			r: graveler.NewGraveler(branchLocker, &testutil.CommittedFake{Err: errTest}, nil,
				&testutil.RefsFake{RefType: graveler.ReferenceTypeCommit, Commits: map[graveler.CommitID]*graveler.Commit{"": {}}}, nil, testutil.NewProtectedBranchesManagerFake(),
			), expectedErr: errTest,
		},
		{
			name: "branch - only staged",
			r: graveler.NewGraveler(branchLocker, &testutil.CommittedFake{Err: graveler.ErrNotFound}, &testutil.StagingFake{Value: &graveler.Value{Identity: []byte("staged")}},
				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, StagingToken: "token1", Commits: map[graveler.CommitID]*graveler.Commit{"": {}}}, nil, testutil.NewProtectedBranchesManagerFake(),
			),
			expectedValueResult: graveler.Value{Identity: []byte("staged")},
		},
		{
			name: "branch - committed and staged",
			r: graveler.NewGraveler(branchLocker, &testutil.CommittedFake{ValuesByKey: map[string]*graveler.Value{"key": {Identity: []byte("committed")}}}, &testutil.StagingFake{Value: &graveler.Value{Identity: []byte("staged")}},
				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, StagingToken: "token1", Commits: map[graveler.CommitID]*graveler.Commit{"": {}}}, nil, testutil.NewProtectedBranchesManagerFake(),
			),
			expectedValueResult: graveler.Value{Identity: []byte("staged")},
		},
		{
			name: "branch - only committed",
			r: graveler.NewGraveler(branchLocker, &testutil.CommittedFake{ValuesByKey: map[string]*graveler.Value{"key": {Identity: []byte("committed")}}}, &testutil.StagingFake{Err: graveler.ErrNotFound},
				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, Commits: map[graveler.CommitID]*graveler.Commit{"": {}}}, nil, testutil.NewProtectedBranchesManagerFake(),
			),
			expectedValueResult: graveler.Value{Identity: []byte("committed")},
		},
		{
			name: "branch - tombstone",
			r: graveler.NewGraveler(branchLocker, &testutil.CommittedFake{ValuesByKey: map[string]*graveler.Value{"key": {Identity: []byte("committed")}}}, &testutil.StagingFake{Value: nil},
				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, StagingToken: "token1", Commits: map[graveler.CommitID]*graveler.Commit{"": {}}}, nil, testutil.NewProtectedBranchesManagerFake(),
			),
			expectedErr: graveler.ErrNotFound,
		},
		{
			name: "branch - staged return error",
			r: graveler.NewGraveler(branchLocker, &testutil.CommittedFake{}, &testutil.StagingFake{Err: errTest},
				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, StagingToken: "token1", Commits: map[graveler.CommitID]*graveler.Commit{"": {}}}, nil, testutil.NewProtectedBranchesManagerFake(),
			),
			expectedErr: errTest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Value, err := tt.r.Get(context.Background(), "", "", []byte("key"))
			if err != tt.expectedErr {
				t.Fatalf("wrong error, expected:%v got:%v", tt.expectedErr, err)
			}
			if err != nil {
				return // err == tt.expected error
			}
			if string(tt.expectedValueResult.Identity) != string(Value.Identity) {
				t.Errorf("wrong Value address, expected:%s got:%s", tt.expectedValueResult.Identity, Value.Identity)
			}
		})
	}
}

func TestGraveler_DiffUncommitted(t *testing.T) {
	conn, _ := tu.GetDB(t, databaseURI)
	branchLocker := ref.NewBranchLocker(conn)
	tests := []struct {
		name            string
		r               *graveler.Graveler
		amount          int
		expectedErr     error
		expectedHasMore bool
		expectedDiff    graveler.DiffIterator
	}{
		{
			name: "no changes",
			r: graveler.NewGraveler(branchLocker, &testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{})},
				&testutil.RefsFake{Branch: &graveler.Branch{CommitID: "c1"}, Commits: map[graveler.CommitID]*graveler.Commit{"c1": {MetaRangeID: "mri1"}}}, nil, testutil.NewProtectedBranchesManagerFake(),
			),
			amount:       10,
			expectedDiff: testutil.NewDiffIter([]graveler.Diff{}),
		},
		{
			name: "added one",
			r: graveler.NewGraveler(branchLocker, &testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{})},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
				&testutil.RefsFake{Branch: &graveler.Branch{CommitID: "c1"}, Commits: map[graveler.CommitID]*graveler.Commit{"c1": {MetaRangeID: "mri1"}}}, nil, testutil.NewProtectedBranchesManagerFake(),
			),
			amount: 10,
			expectedDiff: testutil.NewDiffIter([]graveler.Diff{{
				Key:   graveler.Key("foo/one"),
				Type:  graveler.DiffTypeAdded,
				Value: &graveler.Value{},
			}}),
		},
		{
			name: "changed one",
			r: graveler.NewGraveler(branchLocker, &testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{Identity: []byte("one")}}}), ValuesByKey: map[string]*graveler.Value{"foo/one": {Identity: []byte("one")}}},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{Identity: []byte("one_changed")}}})},
				&testutil.RefsFake{Branch: &graveler.Branch{CommitID: "c1"}, Commits: map[graveler.CommitID]*graveler.Commit{"c1": {MetaRangeID: "mri1"}}}, nil, testutil.NewProtectedBranchesManagerFake(),
			),
			amount: 10,
			expectedDiff: testutil.NewDiffIter([]graveler.Diff{{
				Key:   graveler.Key("foo/one"),
				Type:  graveler.DiffTypeChanged,
				Value: &graveler.Value{Identity: []byte("one_changed")},
			}}),
		},
		{
			name: "removed one",
			r: graveler.NewGraveler(branchLocker, &testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: nil}})},
				&testutil.RefsFake{Branch: &graveler.Branch{CommitID: "c1"}, Commits: map[graveler.CommitID]*graveler.Commit{"c1": {MetaRangeID: "mri1"}}}, nil, testutil.NewProtectedBranchesManagerFake(),
			),
			amount: 10,
			expectedDiff: testutil.NewDiffIter([]graveler.Diff{{
				Key:  graveler.Key("foo/one"),
				Type: graveler.DiffTypeRemoved,
			}}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			diff, err := tt.r.DiffUncommitted(ctx, "repo", "branch")
			if err != tt.expectedErr {
				t.Fatalf("wrong error, expected:%s got:%s", tt.expectedErr, err)
			}
			if err != nil {
				return // err == tt.expectedErr
			}

			// compare iterators
			for diff.Next() {
				if !tt.expectedDiff.Next() {
					t.Fatalf("listing next returned true where expected listing next returned false")
				}
				if diff := deep.Equal(diff.Value(), tt.expectedDiff.Value()); diff != nil {
					t.Errorf("unexpected diff %s", diff)
				}
			}
			if tt.expectedDiff.Next() {
				t.Fatalf("expected listing next returned true where listing next returned false")
			}
		})
	}
}

func TestGraveler_CreateBranch(t *testing.T) {
	conn, _ := tu.GetDB(t, databaseURI)
	branchLocker := ref.NewBranchLocker(conn)
	gravel := graveler.NewGraveler(branchLocker, nil,
		nil,
		&testutil.RefsFake{
			Err:      graveler.ErrNotFound,
			CommitID: "8888888798e3aeface8e62d1c7072a965314b4",
		}, nil, nil,
	)
	_, err := gravel.CreateBranch(context.Background(), "", "", "")
	if err != nil {
		t.Fatal("unexpected error on create branch", err)
	}
	// test create branch when branch exists
	gravel = graveler.NewGraveler(branchLocker, nil,
		nil,
		&testutil.RefsFake{
			Branch: &graveler.Branch{},
		}, nil, nil,
	)
	_, err = gravel.CreateBranch(context.Background(), "", "", "")
	if !errors.Is(err, graveler.ErrBranchExists) {
		t.Fatal("did not get expected error, expected ErrBranchExists")
	}
}

func TestGraveler_UpdateBranch(t *testing.T) {
	conn, _ := tu.GetDB(t, databaseURI)
	branchLocker := ref.NewBranchLocker(conn)
	gravel := graveler.NewGraveler(branchLocker, nil,
		&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
		&testutil.RefsFake{Branch: &graveler.Branch{}}, nil, nil,
	)
	_, err := gravel.UpdateBranch(context.Background(), "", "", "")
	if !errors.Is(err, graveler.ErrConflictFound) {
		t.Fatal("expected update to fail on conflict")
	}
	gravel = graveler.NewGraveler(branchLocker, nil,
		&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{})},
		&testutil.RefsFake{Branch: &graveler.Branch{}}, nil, nil,
	)
	_, err = gravel.UpdateBranch(context.Background(), "", "", "")
	if err != nil {
		t.Fatal("did not expect to get error")
	}
}

func TestGraveler_Commit(t *testing.T) {
	conn, _ := tu.GetDB(t, databaseURI)
	branchLocker := ref.NewBranchLocker(conn)
	expectedCommitID := graveler.CommitID("expectedCommitId")
	expectedRangeID := graveler.MetaRangeID("expectedRangeID")
	values := testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: nil, Value: nil}})
	type fields struct {
		CommittedManager         *testutil.CommittedFake
		StagingManager           *testutil.StagingFake
		RefManager               *testutil.RefsFake
		ProtectedBranchesManager *testutil.ProtectedBranchesManagerFake
	}
	type args struct {
		ctx          context.Context
		repositoryID graveler.RepositoryID
		branchID     graveler.BranchID
		committer    string
		message      string
		metadata     graveler.Metadata
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		want        graveler.CommitID
		expectedErr error
	}{
		{
			name: "valid commit",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{MetaRangeID: expectedRangeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: values},
				RefManager: &testutil.RefsFake{CommitID: expectedCommitID,
					Branch:  &graveler.Branch{CommitID: expectedCommitID},
					Commits: map[graveler.CommitID]*graveler.Commit{expectedCommitID: {MetaRangeID: expectedRangeID}}},
			},
			args: args{
				ctx:          nil,
				repositoryID: "repo",
				branchID:     "branch",
				committer:    "committer",
				message:      "a message",
				metadata:     graveler.Metadata{},
			},
			want:        expectedCommitID,
			expectedErr: nil,
		},
		{
			name: "fail on staging",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{MetaRangeID: expectedRangeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: values, Err: graveler.ErrNotFound},
				RefManager: &testutil.RefsFake{CommitID: expectedCommitID,
					Branch:  &graveler.Branch{CommitID: expectedCommitID},
					Commits: map[graveler.CommitID]*graveler.Commit{expectedCommitID: {MetaRangeID: expectedRangeID}}},
			},
			args: args{
				ctx:          nil,
				repositoryID: "repo",
				branchID:     "branch",
				committer:    "committer",
				message:      "a message",
				metadata:     nil,
			},
			want:        expectedCommitID,
			expectedErr: graveler.ErrNotFound,
		},
		{
			name: "fail on apply",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{MetaRangeID: expectedRangeID, Err: graveler.ErrConflictFound},
				StagingManager:   &testutil.StagingFake{ValueIterator: values},
				RefManager: &testutil.RefsFake{CommitID: expectedCommitID,
					Branch:  &graveler.Branch{CommitID: expectedCommitID},
					Commits: map[graveler.CommitID]*graveler.Commit{expectedCommitID: {MetaRangeID: expectedRangeID}}},
			},
			args: args{
				ctx:          nil,
				repositoryID: "repo",
				branchID:     "branch",
				committer:    "committer",
				message:      "a message",
				metadata:     nil,
			},
			want:        expectedCommitID,
			expectedErr: graveler.ErrConflictFound,
		},
		{
			name: "fail on add commit",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{MetaRangeID: expectedRangeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: values},
				RefManager: &testutil.RefsFake{CommitID: expectedCommitID,
					Branch:    &graveler.Branch{CommitID: expectedCommitID},
					CommitErr: graveler.ErrConflictFound,
					Commits:   map[graveler.CommitID]*graveler.Commit{expectedCommitID: {MetaRangeID: expectedRangeID}}},
			},
			args: args{
				ctx:          nil,
				repositoryID: "repo",
				branchID:     "branch",
				committer:    "committer",
				message:      "a message",
				metadata:     nil,
			},
			want:        expectedCommitID,
			expectedErr: graveler.ErrConflictFound,
		},
		{
			name: "fail on drop",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{MetaRangeID: expectedRangeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: values, DropErr: graveler.ErrNotFound},
				RefManager: &testutil.RefsFake{CommitID: expectedCommitID,
					Branch:  &graveler.Branch{CommitID: expectedCommitID},
					Commits: map[graveler.CommitID]*graveler.Commit{expectedCommitID: {MetaRangeID: expectedRangeID}}},
			},
			args: args{
				ctx:          nil,
				repositoryID: "repo",
				branchID:     "branch",
				committer:    "committer",
				message:      "a message",
				metadata:     graveler.Metadata{},
			},
			want:        expectedCommitID,
			expectedErr: nil,
		},
		{
			name: "fail on protected branch",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{MetaRangeID: expectedRangeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: values},
				RefManager: &testutil.RefsFake{CommitID: expectedCommitID,
					Branch:  &graveler.Branch{CommitID: expectedCommitID},
					Commits: map[graveler.CommitID]*graveler.Commit{expectedCommitID: {MetaRangeID: expectedRangeID}}},
				ProtectedBranchesManager: testutil.NewProtectedBranchesManagerFake("branch"),
			},
			args: args{
				ctx:          nil,
				repositoryID: "repo",
				branchID:     "branch",
				committer:    "committer",
				message:      "a message",
				metadata:     graveler.Metadata{},
			},
			expectedErr: graveler.ErrCommitToProtectedBranch,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expectedCommitID := graveler.CommitID("expectedCommitId")
			expectedRangeID := graveler.MetaRangeID("expectedRangeID")
			values := testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: nil, Value: nil}})
			if tt.fields.ProtectedBranchesManager == nil {
				tt.fields.ProtectedBranchesManager = testutil.NewProtectedBranchesManagerFake()
			}
			g := graveler.NewGraveler(branchLocker, tt.fields.CommittedManager, tt.fields.StagingManager, tt.fields.RefManager, nil, tt.fields.ProtectedBranchesManager)

			got, err := g.Commit(context.Background(), tt.args.repositoryID, tt.args.branchID, graveler.CommitParams{
				Committer: tt.args.committer,
				Message:   tt.args.message,
				Metadata:  tt.args.metadata,
			})
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("unexpected err got = %v, wanted = %v", err, tt.expectedErr)
			}
			if err != nil {
				return
			}
			if diff := deep.Equal(tt.fields.CommittedManager.AppliedData, testutil.AppliedData{
				Values:      values,
				MetaRangeID: expectedRangeID,
			}); diff != nil {
				t.Errorf("unexpected apply data %s", diff)
			}

			if diff := deep.Equal(tt.fields.RefManager.AddedCommit, testutil.AddedCommitData{
				Committer:   tt.args.committer,
				Message:     tt.args.message,
				MetaRangeID: expectedRangeID,
				Parents:     graveler.CommitParents{expectedCommitID},
				Metadata:    graveler.Metadata{},
			}); diff != nil {
				t.Errorf("unexpected added commit %s", diff)
			}
			if !tt.fields.StagingManager.DropCalled && tt.fields.StagingManager.DropErr == nil {
				t.Errorf("expected drop to be called")
			}

			if got != expectedCommitID {
				t.Errorf("got wrong commitID, got = %v, want %v", got, expectedCommitID)
			}
		})
	}
}

func TestGraveler_PreCommitHook(t *testing.T) {
	// prepare graveler
	conn, _ := tu.GetDB(t, databaseURI)
	branchLocker := ref.NewBranchLocker(conn)
	const expectedRangeID = graveler.MetaRangeID("expectedRangeID")
	const expectedCommitID = graveler.CommitID("expectedCommitId")
	committedManager := &testutil.CommittedFake{MetaRangeID: expectedRangeID}
	stagingManager := &testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake(nil)}
	refManager := &testutil.RefsFake{
		CommitID: expectedCommitID,
		Branch:   &graveler.Branch{CommitID: expectedCommitID},
		Commits:  map[graveler.CommitID]*graveler.Commit{expectedCommitID: {MetaRangeID: expectedRangeID}},
	}
	// tests
	errSomethingBad := errors.New("something bad")
	const commitRepositoryID = "repoID"
	const commitBranchID = "branchID"
	const commitCommitter = "committer"
	const commitMessage = "message"
	commitMetadata := graveler.Metadata{"key1": "val1"}
	tests := []struct {
		name string
		hook bool
		err  error
	}{
		{
			name: "without hook",
			hook: false,
			err:  nil,
		},
		{
			name: "hook no error",
			hook: true,
			err:  nil,
		},
		{
			name: "hook error",
			hook: true,
			err:  errSomethingBad,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup
			ctx := context.Background()
			g := graveler.NewGraveler(branchLocker, committedManager, stagingManager, refManager, nil, testutil.NewProtectedBranchesManagerFake())
			h := &Hooks{Err: tt.err}
			if tt.hook {
				g.SetHooksHandler(h)
			}
			// call commit
			_, err := g.Commit(ctx, commitRepositoryID, commitBranchID, graveler.CommitParams{
				Committer: commitCommitter,
				Message:   commitMessage,
				Metadata:  commitMetadata,
			})
			// check err composition
			if !errors.Is(err, tt.err) {
				t.Fatalf("Commit err=%v, expected=%v", err, tt.err)
			}
			var hookErr *graveler.HookAbortError
			if err != nil && !errors.As(err, &hookErr) {
				t.Fatalf("Commit err=%v, expected HookAbortError", err)
			}
			if tt.hook != h.Called {
				t.Fatalf("Commit invalid pre-hook call, %t expected=%t", h.Called, tt.hook)
			}
			if !h.Called {
				return
			}
			if h.RepositoryID != commitRepositoryID {
				t.Errorf("Hook repository '%s', expected '%s'", h.RepositoryID, commitRepositoryID)
			}
			if h.BranchID != commitBranchID {
				t.Errorf("Hook branch '%s', expected '%s'", h.BranchID, commitBranchID)
			}
			if h.Commit.Message != commitMessage {
				t.Errorf("Hook commit message '%s', expected '%s'", h.Commit.Message, commitMessage)
			}
			if diff := deep.Equal(h.Commit.Metadata, commitMetadata); diff != nil {
				t.Error("Hook commit metadata diff:", diff)
			}
		})
	}
}

func TestGraveler_PreMergeHook(t *testing.T) {
	// prepare graveler
	conn, _ := tu.GetDB(t, databaseURI)
	branchLocker := ref.NewBranchLocker(conn)
	const expectedRangeID = graveler.MetaRangeID("expectedRangeID")
	const expectedCommitID = graveler.CommitID("expectedCommitID")
	const destinationCommitID = graveler.CommitID("destinationCommitID")
	const mergeDestination = graveler.BranchID("destinationID")
	committedManager := &testutil.CommittedFake{MetaRangeID: expectedRangeID}
	stagingManager := &testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake(nil)}
	refManager := &testutil.RefsFake{
		CommitID: expectedCommitID,
		Branch:   &graveler.Branch{CommitID: destinationCommitID},
		Refs: map[graveler.Ref]*graveler.ResolvedRef{
			graveler.Ref(mergeDestination): {
				Type:     graveler.ReferenceTypeBranch,
				BranchID: mergeDestination,
				CommitID: destinationCommitID,
			},
		},
		Commits: map[graveler.CommitID]*graveler.Commit{
			expectedCommitID:    {MetaRangeID: expectedRangeID},
			destinationCommitID: {MetaRangeID: expectedRangeID},
		},
	}
	// tests
	errSomethingBad := errors.New("first error")
	const mergeRepositoryID = "repoID"
	const commitCommitter = "committer"
	const mergeMessage = "message"
	mergeMetadata := graveler.Metadata{"key1": "val1"}
	tests := []struct {
		name string
		hook bool
		err  error
	}{
		{
			name: "without hook",
			hook: false,
			err:  nil,
		},
		{
			name: "hook no error",
			hook: true,
			err:  nil,
		},
		{
			name: "hook error",
			hook: true,
			err:  errSomethingBad,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup
			ctx := context.Background()
			g := graveler.NewGraveler(branchLocker, committedManager, stagingManager, refManager, nil, testutil.NewProtectedBranchesManagerFake())
			h := &Hooks{Err: tt.err}
			if tt.hook {
				g.SetHooksHandler(h)
			}
			// call merge
			_, err := g.Merge(ctx, mergeRepositoryID, mergeDestination, expectedCommitID.Ref(), graveler.CommitParams{
				Committer: commitCommitter,
				Message:   mergeMessage,
				Metadata:  mergeMetadata,
			}, "")
			// verify we got an error
			if !errors.Is(err, tt.err) {
				t.Fatalf("Merge err=%v, pre-merge error expected=%v", err, tt.err)
			}
			var hookErr *graveler.HookAbortError
			if err != nil && !errors.As(err, &hookErr) {
				t.Fatalf("Merge err=%v, pre-merge error expected HookAbortError", err)
			}
			if refManager.AddedCommit.MetaRangeID == "" {
				t.Fatalf("Empty MetaRangeID, commit was successful - %+v", refManager.AddedCommit)
			}
			parents := refManager.AddedCommit.Parents
			if len(parents) != 2 {
				t.Fatalf("Merge commit should have 2 parents (%v)", parents)
			}
			if parents[0] != destinationCommitID || parents[1] != expectedCommitID {
				t.Fatalf("Wrong CommitParents order, expected: (%s, %s), got: (%s, %s)", destinationCommitID, expectedCommitID, parents[0], parents[1])
			}
			// verify that calls made until the first error
			if tt.hook != h.Called {
				t.Fatalf("Merge hook h.Called=%t, expected=%t", h.Called, tt.hook)
			}
			if !h.Called {
				return
			}
			if h.RepositoryID != mergeRepositoryID {
				t.Errorf("Hook repository '%s', expected '%s'", h.RepositoryID, mergeRepositoryID)
			}
			if h.BranchID != mergeDestination {
				t.Errorf("Hook branch (destination) '%s', expected '%s'", h.BranchID, mergeDestination)
			}
			if h.SourceRef.String() != expectedCommitID.String() {
				t.Errorf("Hook source '%s', expected '%s'", h.SourceRef, expectedCommitID)
			}
			if h.Commit.Message != mergeMessage {
				t.Errorf("Hook merge message '%s', expected '%s'", h.Commit.Message, mergeMessage)
			}
			if diff := deep.Equal(h.Commit.Metadata, mergeMetadata); diff != nil {
				t.Error("Hook merge metadata diff:", diff)
			}
		})
	}
}

// TestGraveler_MergeInvalidRef test merge with invalid source reference in order
func TestGraveler_MergeInvalidRef(t *testing.T) {
	// prepare graveler
	conn, _ := tu.GetDB(t, databaseURI)
	branchLocker := ref.NewBranchLocker(conn)
	const expectedRangeID = graveler.MetaRangeID("expectedRangeID")
	const destinationCommitID = graveler.CommitID("destinationCommitID")
	const mergeDestination = graveler.BranchID("destinationID")
	committedManager := &testutil.CommittedFake{MetaRangeID: expectedRangeID}
	stagingManager := &testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake(nil)}
	refManager := &testutil.RefsFake{
		Err:    graveler.ErrInvalidRef,
		Branch: &graveler.Branch{CommitID: destinationCommitID},
		Refs: map[graveler.Ref]*graveler.ResolvedRef{
			graveler.Ref(mergeDestination): {
				Type:     graveler.ReferenceTypeBranch,
				BranchID: mergeDestination,
				CommitID: destinationCommitID,
			},
		},
		Commits: map[graveler.CommitID]*graveler.Commit{
			destinationCommitID: {MetaRangeID: expectedRangeID},
		},
	}
	g := graveler.NewGraveler(branchLocker, committedManager, stagingManager, refManager, nil, testutil.NewProtectedBranchesManagerFake())

	// test merge invalid ref
	ctx := context.Background()
	const mergeRepositoryID = "repoID"
	const commitCommitter = "committer"
	const mergeMessage = "message"
	_, err := g.Merge(ctx, mergeRepositoryID, mergeDestination, "unexpectedRef", graveler.CommitParams{
		Committer: commitCommitter,
		Message:   mergeMessage,
		Metadata:  graveler.Metadata{"key1": "val1"},
	}, "")
	if !errors.Is(err, graveler.ErrInvalidRef) {
		t.Fatalf("Merge failed with err=%v, expected ErrInvalidRef", err)
	}
}

func TestGraveler_AddCommitToBranchHead(t *testing.T) {
	conn, _ := tu.GetDB(t, databaseURI)
	branchLocker := ref.NewBranchLocker(conn)
	const (
		expectedCommitID         = graveler.CommitID("expectedCommitId")
		expectedParentCommitID   = graveler.CommitID("expectedParentCommitId")
		unexpectedParentCommitID = graveler.CommitID("unexpectedParentCommitId")
		expectedRangeID          = graveler.MetaRangeID("expectedRangeID")
		expectedRepositoryID     = graveler.RepositoryID("expectedRangeID")
		expectedBranchID         = graveler.BranchID("expectedBranchID")
	)

	type fields struct {
		CommittedManager *testutil.CommittedFake
		StagingManager   *testutil.StagingFake
		RefManager       *testutil.RefsFake
	}
	type args struct {
		ctx          context.Context
		repositoryID graveler.RepositoryID
		branchID     graveler.BranchID
		committer    string
		message      string
		metadata     graveler.Metadata
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		want        graveler.CommitID
		expectedErr error
	}{
		{
			name: "valid commit",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{MetaRangeID: expectedRangeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake(nil)},
				RefManager: &testutil.RefsFake{CommitID: expectedCommitID, Branch: &graveler.Branch{CommitID: expectedParentCommitID},
					Commits: map[graveler.CommitID]*graveler.Commit{
						expectedParentCommitID: {},
					},
				}},
			args: args{
				ctx:          nil,
				repositoryID: "repo",
				branchID:     "branch",
				committer:    "committer",
				message:      "a message",
				metadata:     graveler.Metadata{},
			},
			want:        expectedCommitID,
			expectedErr: nil,
		},
		{
			name: "conflict",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{MetaRangeID: expectedRangeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake(nil)},
				RefManager: &testutil.RefsFake{CommitID: expectedCommitID, Branch: &graveler.Branch{CommitID: unexpectedParentCommitID},
					Commits: map[graveler.CommitID]*graveler.Commit{
						expectedParentCommitID: {},
					},
				}},
			args: args{
				ctx:          nil,
				repositoryID: "repo",
				branchID:     "branch",
				committer:    "committer",
				message:      "a message",
				metadata:     graveler.Metadata{},
			},
			want:        graveler.CommitID(""),
			expectedErr: graveler.ErrCommitNotHeadBranch,
		},
		{
			name: "meta range not found",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{Err: graveler.ErrMetaRangeNotFound},
				StagingManager:   &testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake(nil)},
				RefManager: &testutil.RefsFake{CommitID: expectedParentCommitID,
					Branch:  &graveler.Branch{CommitID: expectedParentCommitID},
					Commits: map[graveler.CommitID]*graveler.Commit{expectedParentCommitID: {MetaRangeID: expectedRangeID}}},
			},
			args: args{
				ctx:          nil,
				repositoryID: "repo",
				branchID:     "branch",
				committer:    "committer",
				message:      "a message",
				metadata:     nil,
			},
			want:        expectedCommitID,
			expectedErr: graveler.ErrMetaRangeNotFound,
		},
		{
			name: "fail on add commit",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{MetaRangeID: expectedRangeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake(nil)},
				RefManager: &testutil.RefsFake{CommitID: expectedCommitID,
					Branch:    &graveler.Branch{CommitID: expectedParentCommitID},
					CommitErr: graveler.ErrConflictFound,
					Commits:   map[graveler.CommitID]*graveler.Commit{expectedParentCommitID: {MetaRangeID: expectedRangeID}}},
			},
			args: args{
				ctx:          nil,
				repositoryID: "repo",
				branchID:     "branch",
				committer:    "committer",
				message:      "a message",
				metadata:     nil,
			},
			want:        expectedCommitID,
			expectedErr: graveler.ErrConflictFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := graveler.NewGraveler(branchLocker, tt.fields.CommittedManager, tt.fields.StagingManager, tt.fields.RefManager, nil, testutil.NewProtectedBranchesManagerFake())
			got, err := g.AddCommitToBranchHead(context.Background(), expectedRepositoryID, expectedBranchID, graveler.Commit{
				Committer:   tt.args.committer,
				Message:     tt.args.message,
				MetaRangeID: expectedRangeID,
				Parents:     graveler.CommitParents{expectedParentCommitID},
				Metadata:    tt.args.metadata,
			})
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("unexpected err got = %v, wanted = %v", err, tt.expectedErr)
			}
			if err != nil {
				return
			}

			if diff := deep.Equal(tt.fields.RefManager.AddedCommit, testutil.AddedCommitData{
				Committer:   tt.args.committer,
				Message:     tt.args.message,
				MetaRangeID: expectedRangeID,
				Parents:     graveler.CommitParents{expectedParentCommitID},
				Metadata:    graveler.Metadata{},
			}); diff != nil {
				t.Errorf("unexpected added commit %s", diff)
			}
			if tt.fields.StagingManager.DropCalled {
				t.Error("Staging manager drop shouldn't be called")
			}

			if got != expectedCommitID {
				t.Errorf("got wrong commitID, got = %v, want %v", got, expectedCommitID)
			}
		})
	}
}

func TestGraveler_AddCommit(t *testing.T) {
	conn, _ := tu.GetDB(t, databaseURI)
	branchLocker := ref.NewBranchLocker(conn)
	const (
		expectedCommitID       = graveler.CommitID("expectedCommitId")
		expectedParentCommitID = graveler.CommitID("expectedParentCommitId")
		expectedRangeID        = graveler.MetaRangeID("expectedRangeID")
		expectedRepositoryID   = graveler.RepositoryID("expectedRepoID")
	)

	type fields struct {
		CommittedManager *testutil.CommittedFake
		StagingManager   *testutil.StagingFake
		RefManager       *testutil.RefsFake
	}
	type args struct {
		ctx            context.Context
		repositoryID   graveler.RepositoryID
		committer      string
		message        string
		metadata       graveler.Metadata
		missingParents bool
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		want        graveler.CommitID
		expectedErr error
	}{
		{
			name: "valid commit",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{MetaRangeID: expectedRangeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake(nil)},
				RefManager: &testutil.RefsFake{CommitID: expectedCommitID, Branch: &graveler.Branch{CommitID: expectedParentCommitID},
					Commits: map[graveler.CommitID]*graveler.Commit{
						expectedParentCommitID: {},
					},
				}},
			args: args{
				ctx:          nil,
				repositoryID: "repo",
				committer:    "committer",
				message:      "a message",
				metadata:     graveler.Metadata{},
			},
			want:        expectedCommitID,
			expectedErr: nil,
		},
		{
			name: "meta range not found",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{Err: graveler.ErrMetaRangeNotFound},
				StagingManager:   &testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake(nil)},
				RefManager: &testutil.RefsFake{CommitID: expectedParentCommitID,
					Branch:  &graveler.Branch{CommitID: expectedParentCommitID},
					Commits: map[graveler.CommitID]*graveler.Commit{expectedParentCommitID: {MetaRangeID: expectedRangeID}}},
			},
			args: args{
				ctx:          nil,
				repositoryID: "repo",
				committer:    "committer",
				message:      "a message",
				metadata:     nil,
			},
			want:        expectedCommitID,
			expectedErr: graveler.ErrMetaRangeNotFound,
		},
		{
			name: "missing parents",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{MetaRangeID: expectedRangeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake(nil)},
				RefManager: &testutil.RefsFake{CommitID: expectedParentCommitID,
					Branch:  &graveler.Branch{CommitID: expectedParentCommitID},
					Commits: map[graveler.CommitID]*graveler.Commit{expectedParentCommitID: {MetaRangeID: expectedRangeID}}},
			},
			args: args{
				ctx:            nil,
				repositoryID:   "repo",
				committer:      "committer",
				message:        "a message",
				metadata:       nil,
				missingParents: true,
			},
			want:        expectedCommitID,
			expectedErr: graveler.ErrAddCommitNoParent,
		},
		{
			name: "fail on add commit",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{MetaRangeID: expectedRangeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake(nil)},
				RefManager: &testutil.RefsFake{CommitID: expectedCommitID,
					Branch:    &graveler.Branch{CommitID: expectedParentCommitID},
					CommitErr: graveler.ErrConflictFound,
					Commits:   map[graveler.CommitID]*graveler.Commit{expectedParentCommitID: {MetaRangeID: expectedRangeID}}},
			},
			args: args{
				ctx:          nil,
				repositoryID: "repo",
				committer:    "committer",
				message:      "a message",
				metadata:     nil,
			},
			want:        expectedCommitID,
			expectedErr: graveler.ErrConflictFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := graveler.NewGraveler(branchLocker, tt.fields.CommittedManager, tt.fields.StagingManager, tt.fields.RefManager, nil, testutil.NewProtectedBranchesManagerFake())
			commit := graveler.Commit{
				Committer:   tt.args.committer,
				Message:     tt.args.message,
				MetaRangeID: expectedRangeID,
				Metadata:    tt.args.metadata,
			}
			if !tt.args.missingParents {
				commit.Parents = graveler.CommitParents{expectedParentCommitID}
			}
			got, err := g.AddCommit(context.Background(), expectedRepositoryID, commit)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("unexpected err got = %v, wanted = %v", err, tt.expectedErr)
			}
			if err != nil {
				return
			}

			if diff := deep.Equal(tt.fields.RefManager.AddedCommit, testutil.AddedCommitData{
				Committer:   tt.args.committer,
				Message:     tt.args.message,
				MetaRangeID: expectedRangeID,
				Parents:     graveler.CommitParents{expectedParentCommitID},
				Metadata:    graveler.Metadata{},
			}); diff != nil {
				t.Errorf("unexpected added commit %s", diff)
			}
			if tt.fields.StagingManager.DropCalled {
				t.Error("Staging manager drop shouldn't be called")
			}

			if got != expectedCommitID {
				t.Errorf("got wrong commitID, got = %v, want %v", got, expectedCommitID)
			}
		})
	}
}

func TestGraveler_Delete(t *testing.T) {
	conn, _ := tu.GetDB(t, databaseURI)
	branchLocker := ref.NewBranchLocker(conn)
	type fields struct {
		CommittedManager graveler.CommittedManager
		StagingManager   *testutil.StagingFake
		RefManager       graveler.RefManager
	}
	type args struct {
		repositoryID graveler.RepositoryID
		branchID     graveler.BranchID
		key          graveler.Key
	}
	tests := []struct {
		name               string
		fields             fields
		args               args
		expectedSetValue   *graveler.ValueRecord
		expectedRemovedKey graveler.Key
		expectedErr        error
	}{
		{
			name: "exists only in committed",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{
					ValuesByKey: map[string]*graveler.Value{"key": {}},
				},
				StagingManager: &testutil.StagingFake{
					Err: graveler.ErrNotFound,
				},
				RefManager: &testutil.RefsFake{
					Branch:  &graveler.Branch{CommitID: "c1"},
					Commits: map[graveler.CommitID]*graveler.Commit{"c1": {}}},
			},
			args: args{
				key: []byte("key"),
			},
			expectedSetValue: &graveler.ValueRecord{
				Key:   []byte("key"),
				Value: nil,
			},
			expectedErr: nil,
		},
		{
			name: "exists in committed and in staging",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{
					ValuesByKey: map[string]*graveler.Value{"key": {}},
				},
				StagingManager: &testutil.StagingFake{
					Value: &graveler.Value{},
				},
				RefManager: &testutil.RefsFake{
					Branch:  &graveler.Branch{CommitID: "c1"},
					Commits: map[graveler.CommitID]*graveler.Commit{"c1": {}},
				},
			},
			args: args{
				key: []byte("key"),
			},
			expectedSetValue: &graveler.ValueRecord{
				Key:   []byte("key"),
				Value: nil,
			},
			expectedErr: nil,
		},
		{
			name: "exists in committed tombstone in staging",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{
					ValuesByKey: map[string]*graveler.Value{"key": {}},
				},
				StagingManager: &testutil.StagingFake{
					Value: nil,
				},
				RefManager: &testutil.RefsFake{
					Branch:  &graveler.Branch{CommitID: "c1"},
					Commits: map[graveler.CommitID]*graveler.Commit{"c1": {}},
				},
			},
			args:        args{},
			expectedErr: graveler.ErrNotFound,
		},
		{
			name: "exists only in staging - commits",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{
					Err: graveler.ErrNotFound,
				},
				StagingManager: &testutil.StagingFake{
					Value: &graveler.Value{},
				},
				RefManager: &testutil.RefsFake{
					Branch:  &graveler.Branch{CommitID: "c1"},
					Commits: map[graveler.CommitID]*graveler.Commit{"c1": {}},
				},
			},
			args: args{
				key: []byte("key"),
			},
			expectedRemovedKey: []byte("key"),
			expectedErr:        nil,
		},
		{
			name: "exists only in staging - no commits",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{},
				StagingManager: &testutil.StagingFake{
					Value: &graveler.Value{},
				},
				RefManager: &testutil.RefsFake{
					Branch:  &graveler.Branch{},
					Commits: map[graveler.CommitID]*graveler.Commit{},
				},
			},
			args: args{
				key: []byte("key"),
			},
			expectedRemovedKey: []byte("key"),
			expectedErr:        nil,
		},
		{
			name: "not in committed not in staging",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{
					Err: graveler.ErrNotFound,
				},
				StagingManager: &testutil.StagingFake{
					Err: graveler.ErrNotFound,
				},
				RefManager: &testutil.RefsFake{
					Branch:  &graveler.Branch{},
					Commits: map[graveler.CommitID]*graveler.Commit{"": {}},
				},
			},
			args:        args{},
			expectedErr: graveler.ErrNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			g := graveler.NewGraveler(branchLocker, tt.fields.CommittedManager, tt.fields.StagingManager, tt.fields.RefManager, nil, testutil.NewProtectedBranchesManagerFake())
			if err := g.Delete(ctx, tt.args.repositoryID, tt.args.branchID, tt.args.key); !errors.Is(err, tt.expectedErr) {
				t.Errorf("Delete() returned unexpected error. got = %v, expected %v", err, tt.expectedErr)
			}
			// validate set on staging
			if diff := deep.Equal(tt.fields.StagingManager.LastSetValueRecord, tt.expectedSetValue); diff != nil {
				t.Errorf("unexpected set value %s", diff)
			}
			// validate removed from staging
			if !bytes.Equal(tt.fields.StagingManager.LastRemovedKey, tt.expectedRemovedKey) {
				t.Errorf("unexpected removed key got = %s, expected = %s ", tt.fields.StagingManager.LastRemovedKey, tt.expectedRemovedKey)
			}
		})
	}
}

func TestGraveler_PreCreateTagHook(t *testing.T) {
	// prepare graveler
	conn, _ := tu.GetDB(t, databaseURI)
	branchLocker := ref.NewBranchLocker(conn)
	const repositoryID = "repoID"
	const expectedRangeID = graveler.MetaRangeID("expectedRangeID")
	const expectedCommitID = graveler.CommitID("expectedCommitID")
	const expectedTagID = graveler.TagID("expectedTagID")
	committedManager := &testutil.CommittedFake{MetaRangeID: expectedRangeID}
	stagingManager := &testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake(nil)}
	refManager := &testutil.RefsFake{
		CommitID: expectedCommitID,
		Branch:   &graveler.Branch{CommitID: expectedCommitID},
		Err:      graveler.ErrTagNotFound,
		Commits: map[graveler.CommitID]*graveler.Commit{
			expectedCommitID: {MetaRangeID: expectedRangeID},
		},
	}
	// tests
	errSomethingBad := errors.New("first error")
	tests := []struct {
		name string
		hook bool
		err  error
	}{
		{
			name: "without hook",
			hook: false,
			err:  nil,
		},
		{
			name: "hook no error",
			hook: true,
			err:  nil,
		},
		{
			name: "hook error",
			hook: true,
			err:  errSomethingBad,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup
			ctx := context.Background()
			g := graveler.NewGraveler(branchLocker, committedManager, stagingManager, refManager, nil, testutil.NewProtectedBranchesManagerFake())
			h := &Hooks{Err: tt.err}
			if tt.hook {
				g.SetHooksHandler(h)
			}

			err := g.CreateTag(ctx, repositoryID, expectedTagID, expectedCommitID)

			// verify we got an error
			if !errors.Is(err, tt.err) {
				t.Fatalf("Create tag err=%v, expected=%v", err, tt.err)
			}
			var hookErr *graveler.HookAbortError
			if err != nil && !errors.As(err, &hookErr) {
				t.Fatalf("Create tag err=%v, expected HookAbortError", err)
			}

			// verify that calls made until the first error
			if tt.hook != h.Called {
				t.Fatalf("Pre-create tag hook h.Called=%t, expected=%t", h.Called, tt.hook)
			}
			if !h.Called {
				return
			}
			if h.RepositoryID != repositoryID {
				t.Errorf("Hook repository '%s', expected '%s'", h.RepositoryID, repositoryID)
			}
			if h.CommitID != expectedCommitID {
				t.Errorf("Hook commit ID '%s', expected '%s'", h.BranchID, expectedCommitID)
			}
			if h.TagID != expectedTagID {
				t.Errorf("Hook tag ID '%s', expected '%s'", h.TagID, expectedTagID)
			}
		})
	}
}

func TestGraveler_PreDeleteTagHook(t *testing.T) {
	// prepare graveler
	conn, _ := tu.GetDB(t, databaseURI)
	branchLocker := ref.NewBranchLocker(conn)
	const repositoryID = "repoID"
	const expectedRangeID = graveler.MetaRangeID("expectedRangeID")
	const expectedCommitID = graveler.CommitID("expectedCommitID")
	const expectedTagID = graveler.TagID("expectedTagID")
	committedManager := &testutil.CommittedFake{MetaRangeID: expectedRangeID}
	stagingManager := &testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake(nil)}
	refManager := &testutil.RefsFake{
		CommitID: expectedCommitID,
		Branch:   &graveler.Branch{CommitID: expectedCommitID},
		Commits: map[graveler.CommitID]*graveler.Commit{
			expectedCommitID: {MetaRangeID: expectedRangeID},
		},
	}
	// tests
	errSomethingBad := errors.New("first error")
	tests := []struct {
		name string
		hook bool
		err  error
	}{
		{
			name: "without hook",
			hook: false,
			err:  nil,
		},
		{
			name: "hook no error",
			hook: true,
			err:  nil,
		},
		{
			name: "hook error",
			hook: true,
			err:  errSomethingBad,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup
			ctx := context.Background()
			expected := expectedCommitID
			refManager.TagCommitID = &expected
			g := graveler.NewGraveler(branchLocker, committedManager, stagingManager, refManager, nil, testutil.NewProtectedBranchesManagerFake())
			h := &Hooks{Err: tt.err}
			if tt.hook {
				g.SetHooksHandler(h)
			}

			err := g.DeleteTag(ctx, repositoryID, expectedTagID)

			// verify we got an error
			if !errors.Is(err, tt.err) {
				t.Fatalf("Delete tag err=%v, expected=%v", err, tt.err)
			}
			var hookErr *graveler.HookAbortError
			if err != nil && !errors.As(err, &hookErr) {
				t.Fatalf("Delete Tag err=%v, expected HookAbortError", err)
			}

			// verify that calls made until the first error
			if tt.hook != h.Called {
				t.Fatalf("Pre delete Tag hook h.Called=%t, expected=%t", h.Called, tt.hook)
			}
			if !h.Called {
				return
			}
			if h.RepositoryID != repositoryID {
				t.Errorf("Hook repository '%s', expected '%s'", h.RepositoryID, repositoryID)
			}
			if h.TagID != expectedTagID {
				t.Errorf("Hook tag ID '%s', expected '%s'", h.TagID, expectedTagID)
			}
		})
	}
}

func TestGraveler_PreCreateBranchHook(t *testing.T) {
	// prepare graveler
	conn, _ := tu.GetDB(t, databaseURI)
	branchLocker := ref.NewBranchLocker(conn)
	const repositoryID = "repoID"
	const expectedRangeID = graveler.MetaRangeID("expectedRangeID")
	const sourceCommitID = graveler.CommitID("sourceCommitID")
	const sourceBranchID = graveler.CommitID("sourceBranchID")
	const newBranchPrefix = "newBranch-"
	committedManager := &testutil.CommittedFake{MetaRangeID: expectedRangeID}
	stagingManager := &testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake(nil)}
	refManager := &testutil.RefsFake{
		Refs: map[graveler.Ref]*graveler.ResolvedRef{graveler.Ref(sourceBranchID): {
			Type:                   graveler.ReferenceTypeBranch,
			BranchID:               graveler.BranchID(sourceBranchID),
			ResolvedBranchModifier: 0,
			CommitID:               sourceCommitID,
			StagingToken:           "",
		}},
	}
	// tests
	errSomethingBad := errors.New("first error")
	tests := []struct {
		name string
		hook bool
		err  error
	}{
		{
			name: "without hook",
			hook: false,
			err:  nil,
		},
		{
			name: "hook no error",
			hook: true,
			err:  nil,
		},
		{
			name: "hook error",
			hook: true,
			err:  errSomethingBad,
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup
			ctx := context.Background()
			g := graveler.NewGraveler(branchLocker, committedManager, stagingManager, refManager, nil, testutil.NewProtectedBranchesManagerFake())
			h := &Hooks{Err: tt.err}
			if tt.hook {
				g.SetHooksHandler(h)
			}

			refManager.Branch = nil // WA for CreateBranch fake logic
			newBranch := newBranchPrefix + strconv.Itoa(i)
			_, err := g.CreateBranch(ctx, repositoryID, graveler.BranchID(newBranch), graveler.Ref(sourceBranchID))

			// verify we got an error
			if !errors.Is(err, tt.err) {
				t.Fatalf("Create branch err=%v, expected=%v", err, tt.err)
			}
			var hookErr *graveler.HookAbortError
			if err != nil && !errors.As(err, &hookErr) {
				t.Fatalf("Create branch err=%v, expected HookAbortError", err)
			}

			// verify that calls made until the first error
			if tt.hook != h.Called {
				t.Fatalf("Pre-create branch hook h.Called=%t, expected=%t", h.Called, tt.hook)
			}
			if !h.Called {
				return
			}
			if h.RepositoryID != repositoryID {
				t.Errorf("Hook repository '%s', expected '%s'", h.RepositoryID, repositoryID)
			}
			if h.CommitID != sourceCommitID {
				t.Errorf("Hook commit ID '%s', expected '%s'", h.BranchID, sourceCommitID)
			}
			if h.BranchID != graveler.BranchID(newBranch) {
				t.Errorf("Hook branch ID '%s', expected '%s'", h.BranchID, newBranch)
			}
		})
	}
}

func TestGraveler_PreDeleteBranchHook(t *testing.T) {
	// prepare graveler
	conn, _ := tu.GetDB(t, databaseURI)
	branchLocker := ref.NewBranchLocker(conn)
	const repositoryID = "repoID"
	const expectedRangeID = graveler.MetaRangeID("expectedRangeID")
	const sourceCommitID = graveler.CommitID("sourceCommitID")
	const sourceBranchID = graveler.CommitID("sourceBranchID")
	committedManager := &testutil.CommittedFake{MetaRangeID: expectedRangeID}
	values := testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: nil, Value: nil}})
	stagingManager := &testutil.StagingFake{ValueIterator: values}
	refManager := &testutil.RefsFake{
		Refs: map[graveler.Ref]*graveler.ResolvedRef{graveler.Ref(sourceBranchID): {
			Type:                   graveler.ReferenceTypeBranch,
			BranchID:               graveler.BranchID(sourceBranchID),
			ResolvedBranchModifier: 0,
			CommitID:               sourceCommitID,
			StagingToken:           "token",
		}},
		Branch:       &graveler.Branch{CommitID: sourceBranchID, StagingToken: "token"},
		StagingToken: "token",
	}
	// tests
	errSomethingBad := errors.New("first error")
	tests := []struct {
		name string
		hook bool
		err  error
	}{
		{
			name: "without hook",
			hook: false,
			err:  nil,
		},
		{
			name: "hook no error",
			hook: true,
			err:  nil,
		},
		{
			name: "hook error",
			hook: true,
			err:  errSomethingBad,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup
			ctx := context.Background()
			g := graveler.NewGraveler(branchLocker, committedManager, stagingManager, refManager, nil, testutil.NewProtectedBranchesManagerFake())
			h := &Hooks{Err: tt.err}
			if tt.hook {
				g.SetHooksHandler(h)
			}

			err := g.DeleteBranch(ctx, repositoryID, graveler.BranchID(sourceBranchID))

			// verify we got an error
			if !errors.Is(err, tt.err) {
				t.Fatalf("Delete branch err=%v, expected=%v", err, tt.err)
			}
			var hookErr *graveler.HookAbortError
			if err != nil && !errors.As(err, &hookErr) {
				t.Fatalf("Delete branch err=%v, expected HookAbortError", err)
			}

			// verify that calls made until the first error
			if tt.hook != h.Called {
				t.Fatalf("Pre-delete branch hook h.Called=%t, expected=%t", h.Called, tt.hook)
			}
			if !h.Called {
				return
			}
			if h.RepositoryID != repositoryID {
				t.Errorf("Hook repository '%s', expected '%s'", h.RepositoryID, repositoryID)
			}
			if h.BranchID != graveler.BranchID(sourceBranchID) {
				t.Errorf("Hook branch ID '%s', expected '%s'", h.BranchID, sourceBranchID)
			}
		})
	}
}
