package graveler_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/testutil"
)

func TestGraveler_List(t *testing.T) {
	tests := []struct {
		name        string
		r           graveler.Graveler
		expectedErr error
		expected    []*graveler.ValueRecord
	}{
		{
			name: "one committed one staged no paths",
			r: graveler.NewGraveler(&testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo"), Value: &graveler.Value{}}})},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("bar"), Value: &graveler.Value{}}})},
				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, Commit: &graveler.Commit{}},
			),
			expected: []*graveler.ValueRecord{{Key: graveler.Key("bar"), Value: &graveler.Value{}}, {Key: graveler.Key("foo"), Value: &graveler.Value{}}},
		},
		{
			name: "same path different file",
			r: graveler.NewGraveler(&testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo"), Value: &graveler.Value{Identity: []byte("original")}}})},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo"), Value: &graveler.Value{Identity: []byte("other")}}})},
				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, Commit: &graveler.Commit{}},
			),
			expected: []*graveler.ValueRecord{{Key: graveler.Key("foo"), Value: &graveler.Value{Identity: []byte("other")}}},
		},
		{
			name: "one committed one staged no paths - with prefix",
			r: graveler.NewGraveler(&testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("prefix/foo"), Value: &graveler.Value{}}})},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("prefix/bar"), Value: &graveler.Value{}}})},
				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, Commit: &graveler.Commit{}},
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
	var ErrTest = errors.New("some kind of err")
	tests := []struct {
		name                string
		r                   graveler.Graveler
		expectedValueResult graveler.Value
		expectedErr         error
	}{
		{
			name: "commit - exists",
			r: graveler.NewGraveler(&testutil.CommittedFake{Value: &graveler.Value{Identity: []byte("committed")}}, nil,
				&testutil.RefsFake{RefType: graveler.ReferenceTypeCommit, Commit: &graveler.Commit{}},
			),
			expectedValueResult: graveler.Value{Identity: []byte("committed")},
		},
		{
			name: "commit - not found",
			r: graveler.NewGraveler(&testutil.CommittedFake{Err: graveler.ErrNotFound}, nil,
				&testutil.RefsFake{RefType: graveler.ReferenceTypeCommit, Commit: &graveler.Commit{}},
			), expectedErr: graveler.ErrNotFound,
		},
		{
			name: "commit - error",
			r: graveler.NewGraveler(&testutil.CommittedFake{Err: ErrTest}, nil,
				&testutil.RefsFake{RefType: graveler.ReferenceTypeCommit, Commit: &graveler.Commit{}},
			), expectedErr: ErrTest,
		},
		{
			name: "branch - only staged",
			r: graveler.NewGraveler(&testutil.CommittedFake{Err: graveler.ErrNotFound}, &testutil.StagingFake{Value: &graveler.Value{Identity: []byte("staged")}},
				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, Commit: &graveler.Commit{}},
			),
			expectedValueResult: graveler.Value{Identity: []byte("staged")},
		},
		{
			name: "branch - committed and staged",
			r: graveler.NewGraveler(&testutil.CommittedFake{Value: &graveler.Value{Identity: []byte("committed")}}, &testutil.StagingFake{Value: &graveler.Value{Identity: []byte("staged")}},

				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, Commit: &graveler.Commit{}},
			),
			expectedValueResult: graveler.Value{Identity: []byte("staged")},
		},
		{
			name: "branch - only committed",
			r: graveler.NewGraveler(&testutil.CommittedFake{Value: &graveler.Value{Identity: []byte("committed")}}, &testutil.StagingFake{Err: graveler.ErrNotFound},

				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, Commit: &graveler.Commit{}},
			),
			expectedValueResult: graveler.Value{Identity: []byte("committed")},
		},
		{
			name: "branch - tombstone",
			r: graveler.NewGraveler(&testutil.CommittedFake{Value: &graveler.Value{Identity: []byte("committed")}}, &testutil.StagingFake{Value: nil},
				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, Commit: &graveler.Commit{}},
			),
			expectedErr: graveler.ErrNotFound,
		},
		{
			name: "branch - staged return error",
			r: graveler.NewGraveler(&testutil.CommittedFake{}, &testutil.StagingFake{Err: ErrTest},
				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, Commit: &graveler.Commit{}},
			),
			expectedErr: ErrTest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Value, err := tt.r.Get(context.Background(), "", "", nil)
			if err != tt.expectedErr {
				t.Fatalf("wrong error, expected:%s got:%s", tt.expectedErr, err)
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
	tests := []struct {
		name            string
		r               graveler.Graveler
		amount          int
		expectedErr     error
		expectedHasMore bool
		expectedDiff    graveler.DiffIterator
	}{
		{
			name: "no changes",
			r: graveler.NewGraveler(&testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}}), Err: graveler.ErrNotFound},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{})},
				&testutil.RefsFake{Branch: &graveler.Branch{}, Commit: &graveler.Commit{}},
			),
			amount:       10,
			expectedDiff: testutil.NewDiffIter([]graveler.Diff{}),
		},
		{
			name: "added one",
			r: graveler.NewGraveler(&testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{}), Err: graveler.ErrNotFound},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
				&testutil.RefsFake{Branch: &graveler.Branch{}, Commit: &graveler.Commit{}},
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
			r: graveler.NewGraveler(&testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
				&testutil.RefsFake{Branch: &graveler.Branch{}, Commit: &graveler.Commit{}},
			),
			amount: 10,
			expectedDiff: testutil.NewDiffIter([]graveler.Diff{{
				Key:   graveler.Key("foo/one"),
				Type:  graveler.DiffTypeChanged,
				Value: &graveler.Value{},
			}}),
		},
		{
			name: "removed one",
			r: graveler.NewGraveler(&testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: nil}})},
				&testutil.RefsFake{Branch: &graveler.Branch{}, Commit: &graveler.Commit{}},
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
			diff, err := tt.r.DiffUncommitted(context.Background(), "repo", "branch")
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
	gravel := graveler.NewGraveler(nil,
		nil,
		&testutil.RefsFake{
			Err: graveler.ErrNotFound,
		},
	)
	_, err := gravel.CreateBranch(context.Background(), "", "", "")
	if err != nil {
		t.Fatal("unexpected error on create branch", err)
	}
	// test create branch when branch exists
	gravel = graveler.NewGraveler(nil,
		nil,
		&testutil.RefsFake{
			Branch: &graveler.Branch{},
		},
	)
	_, err = gravel.CreateBranch(context.Background(), "", "", "")
	if !errors.Is(err, graveler.ErrBranchExists) {
		t.Fatal("did not get expected error, expected ErrBranchExists")
	}
}

func TestGraveler_UpdateBranch(t *testing.T) {
	gravel := graveler.NewGraveler(nil,
		&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
		&testutil.RefsFake{Branch: &graveler.Branch{}},
	)
	_, err := gravel.UpdateBranch(context.Background(), "", "", "")
	if !errors.Is(err, graveler.ErrConflictFound) {
		t.Fatal("expected update to fail on conflict")
	}
	gravel = graveler.NewGraveler(nil,
		&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{})},
		&testutil.RefsFake{Branch: &graveler.Branch{}},
	)
	_, err = gravel.UpdateBranch(context.Background(), "", "", "")
	if err != nil {
		t.Fatal("did not expect to get error")
	}
}

func TestGraveler_Commit(t *testing.T) {
	expectedCommitID := graveler.CommitID("expectedCommitId")
	expectedRangeID := graveler.RangeID("expectedRangeID")
	values := testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: nil, Value: nil}})
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
				CommittedManager: &testutil.CommittedFake{RangeID: expectedRangeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: values},
				RefManager:       &testutil.RefsFake{CommitID: expectedCommitID, Branch: &graveler.Branch{CommitID: expectedCommitID}, Commit: &graveler.Commit{RangeID: expectedRangeID}},
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
				CommittedManager: &testutil.CommittedFake{RangeID: expectedRangeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: values, Err: graveler.ErrNotFound},
				RefManager:       &testutil.RefsFake{CommitID: expectedCommitID, Branch: &graveler.Branch{CommitID: expectedCommitID}, Commit: &graveler.Commit{RangeID: expectedRangeID}},
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
				CommittedManager: &testutil.CommittedFake{RangeID: expectedRangeID, Err: graveler.ErrConflictFound},
				StagingManager:   &testutil.StagingFake{ValueIterator: values},
				RefManager:       &testutil.RefsFake{CommitID: expectedCommitID, Branch: &graveler.Branch{CommitID: expectedCommitID}, Commit: &graveler.Commit{RangeID: expectedRangeID}},
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
				CommittedManager: &testutil.CommittedFake{RangeID: expectedRangeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: values},
				RefManager:       &testutil.RefsFake{CommitID: expectedCommitID, Branch: &graveler.Branch{CommitID: expectedCommitID}, CommitErr: graveler.ErrConflictFound, Commit: &graveler.Commit{RangeID: expectedRangeID}},
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
				CommittedManager: &testutil.CommittedFake{RangeID: expectedRangeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: values, DropErr: graveler.ErrNotFound},
				RefManager:       &testutil.RefsFake{CommitID: expectedCommitID, Branch: &graveler.Branch{CommitID: expectedCommitID}, Commit: &graveler.Commit{RangeID: expectedRangeID}},
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expectedCommitID := graveler.CommitID("expectedCommitId")
			expectedRangeID := graveler.RangeID("expectedRangeID")
			values := testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: nil, Value: nil}})
			g := graveler.NewGraveler(tt.fields.CommittedManager, tt.fields.StagingManager, tt.fields.RefManager)
			//tt.fields.RefManager.Commit = &graveler.Commit{RangeID: expectedRangeID}
			got, err := g.Commit(context.Background(), "", "", tt.args.committer, tt.args.message, tt.args.metadata)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("unexpected err got = %v, wanted = %v", err, tt.expectedErr)
			}
			if err != nil {
				return
			}
			if diff := deep.Equal(tt.fields.CommittedManager.AppliedData, testutil.AppliedData{
				Values:  values,
				RangeID: expectedRangeID,
			}); diff != nil {
				t.Errorf("unexpected apply data %s", diff)
			}

			if diff := deep.Equal(tt.fields.RefManager.AddedCommit, testutil.AddedCommitData{
				Committer: tt.args.committer,
				Message:   tt.args.message,
				RangeID:   expectedRangeID,
				Parents:   graveler.CommitParents{expectedCommitID},
				Metadata:  graveler.Metadata{},
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

func TestGraveler_CommitExistingTree(t *testing.T) {
	const (
		expectedCommitID     = graveler.CommitID("expectedCommitId")
		expectedTreeID       = graveler.TreeID("expectedTreeID")
		expectedBranchID     = graveler.BranchID("expectedBranchID")
		expectedRepositoryID = graveler.RepositoryID("expectedTreeID")
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
			name: "valid existing commit",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{TreeID: expectedTreeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake(nil)},
				RefManager:       &testutil.RefsFake{CommitID: expectedCommitID, Branch: &graveler.Branch{CommitID: expectedCommitID}, Commit: &graveler.Commit{TreeID: expectedTreeID}},
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
			name: "staging is dirty",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{TreeID: expectedTreeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("this-is-key"), Value: &graveler.Value{}}})},
				RefManager:       &testutil.RefsFake{CommitID: expectedCommitID, Branch: &graveler.Branch{CommitID: expectedCommitID}, Commit: &graveler.Commit{TreeID: expectedTreeID}},
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
			expectedErr: graveler.ErrDirtyBranch,
		},
		{
			name: "tree not found",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{Err: graveler.ErrNotFound},
				StagingManager:   &testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake(nil)},
				RefManager:       &testutil.RefsFake{CommitID: expectedCommitID, Branch: &graveler.Branch{CommitID: expectedCommitID}, Commit: &graveler.Commit{TreeID: expectedTreeID}},
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
			expectedErr: graveler.ErrTreeNotFound,
		},
		{
			name: "fail on add commit",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{TreeID: expectedTreeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake(nil)},
				RefManager:       &testutil.RefsFake{CommitID: expectedCommitID, Branch: &graveler.Branch{CommitID: expectedCommitID}, CommitErr: graveler.ErrConflictFound, Commit: &graveler.Commit{TreeID: expectedTreeID}},
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
			g := graveler.NewGraveler(tt.fields.CommittedManager, tt.fields.StagingManager, tt.fields.RefManager)
			got, err := g.CommitExistingTree(context.Background(), expectedRepositoryID, expectedBranchID, expectedTreeID, tt.args.committer, tt.args.message, tt.args.metadata)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("unexpected err got = %v, wanted = %v", err, tt.expectedErr)
			}
			if err != nil {
				return
			}

			if diff := deep.Equal(tt.fields.RefManager.AddedCommit, testutil.AddedCommitData{
				Committer: tt.args.committer,
				Message:   tt.args.message,
				TreeID:    expectedTreeID,
				Parents:   graveler.CommitParents{expectedCommitID},
				Metadata:  graveler.Metadata{},
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
	type fields struct {
		CommittedManager graveler.CommittedManager
		StagingManager   *testutil.StagingFake
		RefManager       graveler.RefManager
	}
	type args struct {
		ctx          context.Context
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
					Value: &graveler.Value{},
				},
				StagingManager: &testutil.StagingFake{
					Err: graveler.ErrNotFound,
				},
				RefManager: &testutil.RefsFake{
					Branch: &graveler.Branch{},
					Commit: &graveler.Commit{},
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
			name: "exists in committed and in staging",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{
					Value: &graveler.Value{},
				},
				StagingManager: &testutil.StagingFake{
					Value: &graveler.Value{},
				},
				RefManager: &testutil.RefsFake{
					Branch: &graveler.Branch{},
					Commit: &graveler.Commit{},
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
					Value: &graveler.Value{},
				},
				StagingManager: &testutil.StagingFake{
					Value: nil,
				},
				RefManager: &testutil.RefsFake{
					Branch: &graveler.Branch{},
					Commit: &graveler.Commit{},
				},
			},
			args:        args{},
			expectedErr: graveler.ErrNotFound,
		},
		{
			name: "exists only in staging",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{
					Err: graveler.ErrNotFound,
				},
				StagingManager: &testutil.StagingFake{
					Value: &graveler.Value{},
				},
				RefManager: &testutil.RefsFake{
					Branch: &graveler.Branch{},
					Commit: &graveler.Commit{},
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
					Branch: &graveler.Branch{},
					Commit: &graveler.Commit{},
				},
			},
			args:        args{},
			expectedErr: graveler.ErrNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := graveler.NewGraveler(tt.fields.CommittedManager, tt.fields.StagingManager, tt.fields.RefManager)
			if err := g.Delete(tt.args.ctx, tt.args.repositoryID, tt.args.branchID, tt.args.key); !errors.Is(err, tt.expectedErr) {
				t.Errorf("Delete() returned unexpected error. got = %v, expected %v", err, tt.expectedErr)
			}
			// validate set on staging
			if diff := deep.Equal(tt.fields.StagingManager.LastSetValueRecord, tt.expectedSetValue); diff != nil {
				t.Errorf("unexpected set value %s", diff)
			}
			// validate removed from staging
			if bytes.Compare(tt.fields.StagingManager.LastRemovedKey, tt.expectedRemovedKey) != 0 {
				t.Errorf("unexpected removed key got = %s, expected = %s ", tt.fields.StagingManager.LastRemovedKey, tt.expectedRemovedKey)
			}

		})
	}
}
