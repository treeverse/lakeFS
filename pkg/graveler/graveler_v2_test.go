package graveler_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/catalog/testutils"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/testutil"
	"github.com/treeverse/lakefs/pkg/kv"
)

var (
	repoID        = graveler.RepositoryID("repo1")
	branch1ID     = graveler.BranchID("branch1")
	branch2ID     = graveler.BranchID("branch2")
	branch3ID     = graveler.BranchID("branch3")
	commit1ID     = graveler.CommitID("commit1")
	commit2ID     = graveler.CommitID("commit2")
	commit3ID     = graveler.CommitID("commit3")
	commit4ID     = graveler.CommitID("commit4")
	stagingToken1 = graveler.StagingToken("st1")
	stagingToken2 = graveler.StagingToken("st2")
	stagingToken3 = graveler.StagingToken("st3")
	stagingToken4 = graveler.StagingToken("st4")
	mr1ID         = graveler.MetaRangeID("mr1")
	mr2ID         = graveler.MetaRangeID("mr2")
	mr3ID         = graveler.MetaRangeID("mr3")
	mr4ID         = graveler.MetaRangeID("mr4")
	repository    = &graveler.RepositoryRecord{
		RepositoryID: repoID,
		Repository: &graveler.Repository{
			StorageNamespace: "mock-sn",
			CreationDate:     time.Now(),
			DefaultBranchID:  branch1ID,
		},
	}
	repositoryRO = &graveler.RepositoryRecord{
		RepositoryID: repoID,
		Repository: &graveler.Repository{
			StorageNamespace: "mock-sn",
			CreationDate:     time.Now(),
			DefaultBranchID:  branch1ID,
			ReadOnly:         true,
		},
	}
	branch1 = graveler.Branch{
		CommitID:     commit1ID,
		StagingToken: stagingToken1,
		SealedTokens: []graveler.StagingToken{stagingToken2, stagingToken3},
	}
	branch3 = graveler.Branch{
		CommitID:                 commit1ID,
		StagingToken:             stagingToken1,
		SealedTokens:             []graveler.StagingToken{stagingToken2, stagingToken3},
		CompactedBaseMetaRangeID: mr2ID,
	}

	commit1       = graveler.Commit{MetaRangeID: mr1ID, Parents: []graveler.CommitID{commit4ID}}
	commit2       = graveler.Commit{MetaRangeID: mr2ID, Parents: []graveler.CommitID{commit4ID}}
	commit3       = graveler.Commit{MetaRangeID: mr3ID}
	commit4       = graveler.Commit{MetaRangeID: mr4ID}
	rawRefBranch  = graveler.RawRef{BaseRef: string(branch1ID)}
	rawRefBranch3 = graveler.RawRef{BaseRef: string(branch3ID)}
	rawRefCommit1 = graveler.RawRef{BaseRef: string(commit1ID)}
	rawRefCommit2 = graveler.RawRef{BaseRef: string(commit2ID)}
	rawRefCommit4 = graveler.RawRef{BaseRef: string(commit4ID)}
	key1          = []byte("some/key/1")
	key2          = []byte("some/key/2")
	value1        = &graveler.Value{Identity: []byte("id1"), Data: []byte("data1")}
	value2        = &graveler.Value{Identity: []byte("id2"), Data: []byte("data2")}
)

func TestGravelerGet(t *testing.T) {
	ctx := context.Background()
	setupGetFromBranch := func(test *testutil.GravelerTest) {
		test.RefManager.EXPECT().ParseRef(graveler.Ref(branch1ID)).Times(1).Return(rawRefBranch, nil)
		test.RefManager.EXPECT().ResolveRawRef(ctx, repository, rawRefBranch).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeBranch, BranchRecord: graveler.BranchRecord{BranchID: branch1ID, Branch: &branch1}}, nil)
	}

	setupGetFromCommit := func(test *testutil.GravelerTest) {
		test.RefManager.EXPECT().ParseRef(graveler.Ref(commit1ID)).Times(1).Return(rawRefCommit1, nil)
		test.RefManager.EXPECT().ResolveRawRef(ctx, repository, rawRefCommit1).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeCommit, BranchRecord: graveler.BranchRecord{Branch: &graveler.Branch{CommitID: commit1ID}}}, nil)
	}

	t.Run("get from branch - staging", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		setupGetFromBranch(test)

		test.StagingManager.EXPECT().Get(ctx, stagingToken1, key1).Times(1).Return(value1, nil)

		val, err := test.Sut.Get(ctx, repository, graveler.Ref(branch1ID), key1)

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, value1, val)
	})

	t.Run("get from branch - commit", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		setupGetFromBranch(test)

		test.StagingManager.EXPECT().Get(ctx, stagingToken1, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.StagingManager.EXPECT().Get(ctx, stagingToken2, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.StagingManager.EXPECT().Get(ctx, stagingToken3, key1).Times(1).Return(nil, graveler.ErrNotFound)

		test.RefManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(1).Return(&commit1, nil)
		test.CommittedManager.EXPECT().Get(ctx, repository.StorageNamespace, commit1.MetaRangeID, key1).Times(1).Return(value1, nil)

		val, err := test.Sut.Get(ctx, repository, graveler.Ref(branch1ID), key1)

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, value1, val)
	})

	t.Run("get from branch - staging only flag", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		setupGetFromBranch(test)

		test.StagingManager.EXPECT().Get(ctx, stagingToken1, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.StagingManager.EXPECT().Get(ctx, stagingToken2, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.StagingManager.EXPECT().Get(ctx, stagingToken3, key1).Times(1).Return(nil, graveler.ErrNotFound)

		val, err := test.Sut.Get(ctx, repository, graveler.Ref(branch1ID), key1, graveler.WithStageOnly(true))

		require.Error(t, graveler.ErrNotFound, err)
		require.Nil(t, val)
	})

	t.Run("get from branch - staging only flag different in commit", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		setupGetFromBranch(test)

		test.StagingManager.EXPECT().Get(ctx, stagingToken1, key1).Times(1).Return(value1, nil)

		test.RefManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(1).Return(&commit1, nil)
		test.CommittedManager.EXPECT().Get(ctx, repository.StorageNamespace, commit1.MetaRangeID, key1).Times(1).Return(value2, nil)

		val, err := test.Sut.Get(ctx, repository, graveler.Ref(branch1ID), key1, graveler.WithStageOnly(true))

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, value1, val)
	})

	t.Run("get from branch - staging only flag same in staging and in commit", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		setupGetFromBranch(test)

		test.StagingManager.EXPECT().Get(ctx, stagingToken1, key1).Times(1).Return(value1, nil)

		test.RefManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(1).Return(&commit1, nil)
		test.CommittedManager.EXPECT().Get(ctx, repository.StorageNamespace, commit1.MetaRangeID, key1).Times(1).Return(value1, nil)

		val, err := test.Sut.Get(ctx, repository, graveler.Ref(branch1ID), key1, graveler.WithStageOnly(true))

		require.Error(t, graveler.ErrNotFound, err)
		require.Nil(t, val)
	})

	t.Run("get from branch - compacted", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		test.RefManager.EXPECT().ParseRef(graveler.Ref(branch3ID)).Times(1).Return(rawRefBranch3, nil)
		test.RefManager.EXPECT().ResolveRawRef(ctx, repository, rawRefBranch3).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeBranch, BranchRecord: graveler.BranchRecord{BranchID: branch3ID, Branch: &branch3}}, nil)

		test.StagingManager.EXPECT().Get(ctx, stagingToken1, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.StagingManager.EXPECT().Get(ctx, stagingToken2, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.StagingManager.EXPECT().Get(ctx, stagingToken3, key1).Times(1).Return(nil, graveler.ErrNotFound)

		test.CommittedManager.EXPECT().Get(ctx, repository.StorageNamespace, mr2ID, key1).Times(1).Return(value1, nil)

		val, err := test.Sut.Get(ctx, repository, graveler.Ref(branch3ID), key1)

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, value1, val)
	})

	t.Run("get from branch - compacted with staging only flag when get object committed", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		test.RefManager.EXPECT().ParseRef(graveler.Ref(branch3ID)).Times(1).Return(rawRefBranch3, nil)
		test.RefManager.EXPECT().ResolveRawRef(ctx, repository, rawRefBranch3).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeBranch, BranchRecord: graveler.BranchRecord{BranchID: branch3ID, Branch: &branch3}}, nil)

		test.StagingManager.EXPECT().Get(ctx, stagingToken1, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.StagingManager.EXPECT().Get(ctx, stagingToken2, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.StagingManager.EXPECT().Get(ctx, stagingToken3, key1).Times(1).Return(nil, graveler.ErrNotFound)

		test.CommittedManager.EXPECT().Get(ctx, repository.StorageNamespace, mr2ID, key1).Times(1).Return(value1, nil)

		test.RefManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(1).Return(&commit1, nil)
		test.CommittedManager.EXPECT().Get(ctx, repository.StorageNamespace, commit1.MetaRangeID, key1).Times(1).Return(value1, nil)

		val, err := test.Sut.Get(ctx, repository, graveler.Ref(branch3ID), key1, graveler.WithStageOnly(true))

		require.Error(t, graveler.ErrNotFound, err)
		require.Nil(t, val)
	})

	t.Run("get from branch - compacted with staging only flag when get object different in commit", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		test.RefManager.EXPECT().ParseRef(graveler.Ref(branch3ID)).Times(1).Return(rawRefBranch3, nil)
		test.RefManager.EXPECT().ResolveRawRef(ctx, repository, rawRefBranch3).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeBranch, BranchRecord: graveler.BranchRecord{BranchID: branch3ID, Branch: &branch3}}, nil)

		test.StagingManager.EXPECT().Get(ctx, stagingToken1, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.StagingManager.EXPECT().Get(ctx, stagingToken2, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.StagingManager.EXPECT().Get(ctx, stagingToken3, key1).Times(1).Return(nil, graveler.ErrNotFound)

		test.CommittedManager.EXPECT().Get(ctx, repository.StorageNamespace, mr2ID, key1).Times(1).Return(value1, nil)
		test.RefManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(1).Return(&commit1, nil)

		test.CommittedManager.EXPECT().Get(ctx, repository.StorageNamespace, commit1.MetaRangeID, key1).Times(1).Return(value2, nil)
		val, err := test.Sut.Get(ctx, repository, graveler.Ref(branch3ID), key1, graveler.WithStageOnly(true))

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, value1, val)
	})

	t.Run("get from branch - compacted with staging only flag when object is not committed", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		test.RefManager.EXPECT().ParseRef(graveler.Ref(branch3ID)).Times(1).Return(rawRefBranch3, nil)
		test.RefManager.EXPECT().ResolveRawRef(ctx, repository, rawRefBranch3).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeBranch, BranchRecord: graveler.BranchRecord{BranchID: branch3ID, Branch: &branch3}}, nil)

		test.StagingManager.EXPECT().Get(ctx, stagingToken1, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.StagingManager.EXPECT().Get(ctx, stagingToken2, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.StagingManager.EXPECT().Get(ctx, stagingToken3, key1).Times(1).Return(nil, graveler.ErrNotFound)

		test.CommittedManager.EXPECT().Get(ctx, repository.StorageNamespace, mr2ID, key1).Times(1).Return(value1, nil)
		test.RefManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(1).Return(&commit1, nil)

		test.CommittedManager.EXPECT().Get(ctx, repository.StorageNamespace, commit1.MetaRangeID, key1).Times(1).Return(nil, graveler.ErrNotFound)
		val, err := test.Sut.Get(ctx, repository, graveler.Ref(branch3ID), key1, graveler.WithStageOnly(true))

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, value1, val)
	})

	t.Run("get from branch - staging tombstone", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		setupGetFromBranch(test)

		test.StagingManager.EXPECT().Get(ctx, stagingToken1, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.StagingManager.EXPECT().Get(ctx, stagingToken2, key1).Times(1).Return(nil, nil)

		val, err := test.Sut.Get(ctx, repository, graveler.Ref(branch1ID), key1)

		require.Error(t, graveler.ErrNotFound, err)
		require.Nil(t, val)
	})

	t.Run("get from branch - not found in compacted", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		test.RefManager.EXPECT().ParseRef(graveler.Ref(branch3ID)).Times(1).Return(rawRefBranch3, nil)
		test.RefManager.EXPECT().ResolveRawRef(ctx, repository, rawRefBranch3).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeBranch, BranchRecord: graveler.BranchRecord{BranchID: branch3ID, Branch: &branch3}}, nil)

		test.StagingManager.EXPECT().Get(ctx, stagingToken1, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.StagingManager.EXPECT().Get(ctx, stagingToken2, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.StagingManager.EXPECT().Get(ctx, stagingToken3, key1).Times(1).Return(nil, graveler.ErrNotFound)

		test.CommittedManager.EXPECT().Get(ctx, repository.StorageNamespace, mr2ID, key1).Times(1).Return(nil, graveler.ErrNotFound)

		val, err := test.Sut.Get(ctx, repository, graveler.Ref(branch3ID), key1)

		require.Error(t, graveler.ErrNotFound, err)
		require.Nil(t, val)
	})

	t.Run("get from branch - not found", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		setupGetFromBranch(test)

		test.StagingManager.EXPECT().Get(ctx, stagingToken1, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.StagingManager.EXPECT().Get(ctx, stagingToken2, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.StagingManager.EXPECT().Get(ctx, stagingToken3, key1).Times(1).Return(nil, graveler.ErrNotFound)

		test.RefManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(1).Return(&commit1, nil)
		test.CommittedManager.EXPECT().Get(ctx, repository.StorageNamespace, commit1.MetaRangeID, key1).Times(1).Return(nil, graveler.ErrNotFound)

		val, err := test.Sut.Get(ctx, repository, graveler.Ref(branch1ID), key1)

		require.Error(t, graveler.ErrNotFound, err)
		require.Nil(t, val)
	})

	t.Run("get from commit", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		setupGetFromCommit(test)

		test.RefManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(1).Return(&commit1, nil)
		test.CommittedManager.EXPECT().Get(ctx, repository.StorageNamespace, commit1.MetaRangeID, key1).Times(1).Return(value1, nil)

		val, err := test.Sut.Get(ctx, repository, graveler.Ref(commit1ID), key1)

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, value1, val)
	})

	t.Run("get from commit - not found", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		setupGetFromCommit(test)

		test.RefManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(1).Return(&commit1, nil)
		test.CommittedManager.EXPECT().Get(ctx, repository.StorageNamespace, commit1.MetaRangeID, key1).Times(1).Return(nil, graveler.ErrNotFound)

		val, err := test.Sut.Get(ctx, repository, graveler.Ref(commit1ID), key1)

		require.Error(t, graveler.ErrNotFound, err)
		require.Nil(t, val)
	})
}

func TestGravelerMerge(t *testing.T) {
	ctx := context.Background()

	firstUpdateBranch := func(test *testutil.GravelerTest) {
		test.RefManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			Do(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				branchTest := branch1
				updatedBranch, err := f(&branchTest)
				require.NoError(t, err)
				require.Equal(t, []graveler.StagingToken{stagingToken1, stagingToken2, stagingToken3}, updatedBranch.SealedTokens)
				require.NotEmpty(t, updatedBranch.StagingToken)
				require.NotEqual(t, stagingToken1, updatedBranch.StagingToken)
				return nil
			}).Times(1)
	}
	emptyStagingTokenCombo := func(test *testutil.GravelerTest, numReps int) {
		test.StagingManager.EXPECT().List(ctx, stagingToken1, gomock.Any()).Times(numReps).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{{
			Key:   key1,
			Value: nil, // tombstone
		}}))
		test.StagingManager.EXPECT().List(ctx, stagingToken2, gomock.Any()).Times(numReps).Return(testutils.NewFakeValueIterator(nil))
		test.StagingManager.EXPECT().List(ctx, stagingToken3, gomock.Any()).Times(numReps).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{{
			Key:   key1,
			Value: value1,
		}}))
	}
	t.Run("merge successful", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		firstUpdateBranch(test)
		emptyStagingTokenCombo(test, 2)
		test.RefManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(3).Return(&commit1, nil)
		test.CommittedManager.EXPECT().List(ctx, repository.StorageNamespace, mr1ID).Times(2).Return(testutils.NewFakeValueIterator(nil), nil)
		test.RefManager.EXPECT().ParseRef(graveler.Ref(branch2ID)).Times(1).Return(rawRefCommit2, nil)
		test.RefManager.EXPECT().ParseRef(graveler.Ref(branch1ID)).Times(1).Return(rawRefCommit1, nil)
		test.RefManager.EXPECT().ResolveRawRef(ctx, repository, rawRefCommit2).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeCommit, BranchRecord: graveler.BranchRecord{Branch: &graveler.Branch{CommitID: commit2ID}}}, nil)
		test.RefManager.EXPECT().ResolveRawRef(ctx, repository, rawRefCommit1).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeCommit, BranchRecord: graveler.BranchRecord{Branch: &graveler.Branch{CommitID: commit1ID}}}, nil)
		test.RefManager.EXPECT().GetCommit(ctx, repository, commit2ID).Times(1).Return(&commit2, nil)
		test.RefManager.EXPECT().FindMergeBase(ctx, repository, commit2ID, commit1ID).Times(1).Return(&commit3, nil)
		test.CommittedManager.EXPECT().Merge(ctx, repository.StorageNamespace, mr1ID, mr2ID, mr3ID, graveler.MergeStrategyNone, []graveler.SetOptionsFunc{}).Times(1).Return(mr4ID, nil)
		test.RefManager.EXPECT().AddCommit(ctx, repository, gomock.Any()).DoAndReturn(func(ctx context.Context, repository *graveler.RepositoryRecord, commit graveler.Commit) (graveler.CommitID, error) {
			require.Equal(t, mr4ID, commit.MetaRangeID)
			return commit4ID, nil
		}).Times(1)
		test.RefManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			Do(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				branchTest := &graveler.Branch{StagingToken: stagingToken4, CommitID: commit1ID, SealedTokens: []graveler.StagingToken{stagingToken1, stagingToken2, stagingToken3}}
				updatedBranch, err := f(branchTest)
				require.NoError(t, err)
				require.Equal(t, []graveler.StagingToken{}, updatedBranch.SealedTokens)
				require.NotEmpty(t, updatedBranch.StagingToken)
				require.Equal(t, commit4ID, updatedBranch.CommitID)
				return nil
			}).Times(1)
		test.StagingManager.EXPECT().DropAsync(ctx, stagingToken1).Times(1)
		test.StagingManager.EXPECT().DropAsync(ctx, stagingToken2).Times(1)
		test.StagingManager.EXPECT().DropAsync(ctx, stagingToken3).Times(1)

		val, err := test.Sut.Merge(ctx, repository, branch1ID, graveler.Ref(branch2ID), graveler.CommitParams{Metadata: graveler.Metadata{}}, "")

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, commit4ID, graveler.CommitID(val.Ref()))
	})

	t.Run("merge dirty destination while updating tokens", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		test.RefManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				branchTest := branch1
				updatedBranch, err := f(&branchTest)
				require.Error(t, err)
				require.Nil(t, updatedBranch)
				return err
			}).Times(1)

		test.StagingManager.EXPECT().List(ctx, stagingToken1, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{{
			Key:   key1,
			Value: nil, // tombstone
		}}))
		test.StagingManager.EXPECT().List(ctx, stagingToken2, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{{
			Key:   key2,
			Value: value2,
		}}))
		test.StagingManager.EXPECT().List(ctx, stagingToken3, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{{
			Key:   key1,
			Value: value1,
		}}))
		test.RefManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(1).Return(&commit1, nil)
		test.CommittedManager.EXPECT().List(ctx, repository.StorageNamespace, mr1ID).Times(1).Return(testutils.NewFakeValueIterator(nil), nil)

		val, err := test.Sut.Merge(ctx, repository, branch1ID, graveler.Ref(branch2ID), graveler.CommitParams{Metadata: graveler.Metadata{}}, "")
		require.Equal(t, graveler.ErrDirtyBranch, err)
		require.Equal(t, graveler.CommitID(""), val)
	})

	t.Run("merge dirty compacted", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)

		test.RefManager.EXPECT().BranchUpdate(ctx, repository, branch3ID, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				branchTest := branch3
				updatedBranch, err := f(&branchTest)
				require.Error(t, err)
				require.Nil(t, updatedBranch)
				return err
			}).Times(1)
		test.RefManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(1).Return(&commit1, nil)
		test.CommittedManager.EXPECT().List(ctx, repository.StorageNamespace, mr1ID).Times(1).Return(testutils.NewFakeValueIterator(nil), nil)
		test.CommittedManager.EXPECT().Diff(ctx, repository.StorageNamespace, mr1ID, mr2ID).Times(1).Return(testutil.NewDiffIter([]graveler.Diff{{Key: key1, Type: graveler.DiffTypeRemoved}}), nil)

		test.StagingManager.EXPECT().List(ctx, stagingToken1, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator(nil))
		test.StagingManager.EXPECT().List(ctx, stagingToken2, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator(nil))
		test.StagingManager.EXPECT().List(ctx, stagingToken3, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator(nil))

		val, err := test.Sut.Merge(ctx, repository, branch3ID, graveler.Ref(branch2ID), graveler.CommitParams{Metadata: graveler.Metadata{}}, "")

		require.Equal(t, graveler.ErrDirtyBranch, err)
		require.Equal(t, graveler.CommitID(""), val)
	})

	t.Run("merge successful with branchUpdate retry", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)

		// upgrade graveler branch update back-off to shorten test duration
		const updateRetryDuration = 200 * time.Millisecond
		test.Sut.BranchUpdateBackOff = backoff.NewConstantBackOff(updateRetryDuration)

		firstUpdateBranch(test)
		emptyStagingTokenCombo(test, 2)
		test.RefManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(3).Return(&commit1, nil)
		test.CommittedManager.EXPECT().List(ctx, repository.StorageNamespace, mr1ID).Times(2).Return(testutils.NewFakeValueIterator(nil), nil)
		test.RefManager.EXPECT().ParseRef(graveler.Ref(branch2ID)).Times(1).Return(rawRefCommit2, nil)
		test.RefManager.EXPECT().ParseRef(graveler.Ref(branch1ID)).Times(1).Return(rawRefCommit1, nil)
		test.RefManager.EXPECT().ResolveRawRef(ctx, repository, rawRefCommit2).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeCommit, BranchRecord: graveler.BranchRecord{Branch: &graveler.Branch{CommitID: commit2ID}}}, nil)
		test.RefManager.EXPECT().ResolveRawRef(ctx, repository, rawRefCommit1).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeCommit, BranchRecord: graveler.BranchRecord{Branch: &graveler.Branch{CommitID: commit1ID}}}, nil)
		test.RefManager.EXPECT().GetCommit(ctx, repository, commit2ID).Times(1).Return(&commit2, nil)
		test.RefManager.EXPECT().FindMergeBase(ctx, repository, commit2ID, commit1ID).Times(1).Return(&commit3, nil)
		test.CommittedManager.EXPECT().Merge(ctx, repository.StorageNamespace, mr1ID, mr2ID, mr3ID, graveler.MergeStrategyNone, []graveler.SetOptionsFunc{}).Times(1).Return(mr4ID, nil)
		test.RefManager.EXPECT().AddCommit(ctx, repository, gomock.Any()).DoAndReturn(func(ctx context.Context, repository *graveler.RepositoryRecord, commit graveler.Commit) (graveler.CommitID, error) {
			require.Equal(t, mr4ID, commit.MetaRangeID)
			return commit4ID, nil
		}).Times(1)
		test.RefManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				return kv.ErrPredicateFailed
			}).Times(graveler.BranchUpdateMaxTries - 1)
		test.RefManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			Do(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				branchTest := &graveler.Branch{StagingToken: stagingToken4, CommitID: commit1ID, SealedTokens: []graveler.StagingToken{stagingToken1, stagingToken2, stagingToken3}}
				updatedBranch, err := f(branchTest)
				require.NoError(t, err)
				require.Equal(t, []graveler.StagingToken{}, updatedBranch.SealedTokens)
				require.NotEmpty(t, updatedBranch.StagingToken)
				require.Equal(t, commit4ID, updatedBranch.CommitID)
				return nil
			}).Times(1)
		test.StagingManager.EXPECT().DropAsync(ctx, stagingToken1).Times(1)
		test.StagingManager.EXPECT().DropAsync(ctx, stagingToken2).Times(1)
		test.StagingManager.EXPECT().DropAsync(ctx, stagingToken3).Times(1)

		val, err := test.Sut.Merge(ctx, repository, branch1ID, graveler.Ref(branch2ID), graveler.CommitParams{Metadata: graveler.Metadata{}}, "")

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, commit4ID, graveler.CommitID(val.Ref()))
	})

	t.Run("merge fails due to BranchUpdate retries exhaustion", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		firstUpdateBranch(test)
		emptyStagingTokenCombo(test, 1)
		test.RefManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(1).Return(&commit1, nil)
		test.CommittedManager.EXPECT().List(ctx, repository.StorageNamespace, mr1ID).Times(1).Return(testutils.NewFakeValueIterator(nil), nil)
		test.RefManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				return kv.ErrPredicateFailed
			}).Times(graveler.BranchUpdateMaxTries)

		val, err := test.Sut.Merge(ctx, repository, branch1ID, graveler.Ref(branch2ID), graveler.CommitParams{Metadata: graveler.Metadata{}}, "")

		require.ErrorIs(t, err, graveler.ErrTooManyTries)
		require.Empty(t, val)
	})
}

func TestGravelerRevert(t *testing.T) {
	ctx := context.Background()

	firstUpdateBranch := func(test *testutil.GravelerTest) {
		test.RefManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			Do(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				branchTest := branch1
				updatedBranch, err := f(&branchTest)
				require.NoError(t, err)
				require.Equal(t, []graveler.StagingToken{stagingToken1, stagingToken2, stagingToken3}, updatedBranch.SealedTokens)
				require.NotEmpty(t, updatedBranch.StagingToken)
				require.NotEqual(t, stagingToken1, updatedBranch.StagingToken)
				return nil
			}).Times(1)
	}
	emptyStagingTokenCombo := func(test *testutil.GravelerTest, times int) {
		test.StagingManager.EXPECT().List(ctx, stagingToken1, gomock.Any()).Times(times).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{{
			Key:   key1,
			Value: nil, // tombstone
		}}))
		test.StagingManager.EXPECT().List(ctx, stagingToken2, gomock.Any()).Times(times).Return(testutils.NewFakeValueIterator(nil))
		test.StagingManager.EXPECT().List(ctx, stagingToken3, gomock.Any()).Times(times).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{{
			Key:   key1,
			Value: value1,
		}}))
	}
	dirtyStagingTokenCombo := func(test *testutil.GravelerTest) {
		test.StagingManager.EXPECT().List(ctx, stagingToken1, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator(nil))
		test.StagingManager.EXPECT().List(ctx, stagingToken2, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator(nil))
		test.StagingManager.EXPECT().List(ctx, stagingToken3, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{{
			Key:   key1,
			Value: value1,
		}}))
	}

	setupSuccessfulRevertExpectations := func(test *testutil.GravelerTest) {
		test.RefManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(3).Return(&commit1, nil)
		test.CommittedManager.EXPECT().List(ctx, repository.StorageNamespace, mr1ID).Times(2).Return(testutils.NewFakeValueIterator(nil), nil)
		test.RefManager.EXPECT().ParseRef(graveler.Ref(commit2ID)).Times(1).Return(rawRefCommit2, nil)
		test.RefManager.EXPECT().ParseRef(graveler.Ref(commit1ID)).Times(1).Return(rawRefCommit1, nil)
		test.RefManager.EXPECT().ParseRef(graveler.Ref(commit4ID)).Times(1).Return(rawRefCommit4, nil)
		test.RefManager.EXPECT().ResolveRawRef(ctx, repository, rawRefCommit2).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeCommit, BranchRecord: graveler.BranchRecord{Branch: &graveler.Branch{CommitID: commit2ID}}}, nil)
		test.RefManager.EXPECT().ResolveRawRef(ctx, repository, rawRefCommit1).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeCommit, BranchRecord: graveler.BranchRecord{Branch: &graveler.Branch{CommitID: commit1ID}}}, nil)
		test.RefManager.EXPECT().ResolveRawRef(ctx, repository, rawRefCommit4).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeCommit, BranchRecord: graveler.BranchRecord{Branch: &graveler.Branch{CommitID: commit4ID}}}, nil)
		test.RefManager.EXPECT().GetCommit(ctx, repository, commit2ID).Times(1).Return(&commit2, nil)
		test.RefManager.EXPECT().GetCommit(ctx, repository, commit4ID).Times(1).Return(&commit4, nil)
		test.CommittedManager.EXPECT().Merge(ctx, repository.StorageNamespace, mr1ID, mr4ID, mr2ID, graveler.MergeStrategyNone, []graveler.SetOptionsFunc{}).Times(1).Return(mr3ID, nil)
		test.RefManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			Do(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				branchTest := &graveler.Branch{StagingToken: stagingToken4, CommitID: commit1ID, SealedTokens: []graveler.StagingToken{stagingToken1, stagingToken2, stagingToken3}}
				updatedBranch, err := f(branchTest)
				require.NoError(t, err)
				require.Equal(t, []graveler.StagingToken{}, updatedBranch.SealedTokens)
				require.NotEmpty(t, updatedBranch.StagingToken)
				require.Equal(t, commit3ID, updatedBranch.CommitID)
				return nil
			}).Times(1)
		test.StagingManager.EXPECT().DropAsync(ctx, stagingToken1).Times(1)
		test.StagingManager.EXPECT().DropAsync(ctx, stagingToken2).Times(1)
		test.StagingManager.EXPECT().DropAsync(ctx, stagingToken3).Times(1)
	}

	t.Run("revert successful", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		firstUpdateBranch(test)
		emptyStagingTokenCombo(test, 2)

		setupSuccessfulRevertExpectations(test)
		test.RefManager.EXPECT().AddCommit(ctx, repository, gomock.Any()).DoAndReturn(func(ctx context.Context, repository *graveler.RepositoryRecord, commit graveler.Commit) (graveler.CommitID, error) {
			require.Equal(t, mr3ID, commit.MetaRangeID)
			return commit3ID, nil
		}).Times(1)

		val, err := test.Sut.Revert(ctx, repository, branch1ID, graveler.Ref(commit2ID), 0, graveler.CommitParams{}, &graveler.CommitOverrides{})

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, commit3ID, graveler.CommitID(val.Ref()))
	})

	t.Run("revert override commit fields", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		firstUpdateBranch(test)
		emptyStagingTokenCombo(test, 2)

		commitParams := graveler.CommitParams{
			Message: "original message",
			Metadata: map[string]string{
				"originalKey": "originalValue",
			},
		}

		commitOverrides := graveler.CommitOverrides{
			Message: "override message",
			Metadata: map[string]string{
				"originalKey": "overrideValue",
				"newKey":      "newValue",
			},
		}

		setupSuccessfulRevertExpectations(test)
		test.RefManager.EXPECT().AddCommit(ctx, repository, gomock.Any()).DoAndReturn(func(ctx context.Context, repository *graveler.RepositoryRecord, commit graveler.Commit) (graveler.CommitID, error) {
			require.Equal(t, mr3ID, commit.MetaRangeID)
			require.Equal(t, commitOverrides.Message, commit.Message)
			require.Equal(t, commitOverrides.Metadata, commit.Metadata)
			return commit3ID, nil
		}).Times(1)

		val, err := test.Sut.Revert(ctx, repository, branch1ID, graveler.Ref(commit2ID), 0, commitParams, &commitOverrides)

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, commit3ID, graveler.CommitID(val.Ref()))
	})

	t.Run("revert partially override commit fields", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		firstUpdateBranch(test)
		emptyStagingTokenCombo(test, 2)

		commitParams := graveler.CommitParams{
			Message: "original message",
			Metadata: map[string]string{
				"originalKey": "originalValue",
			},
		}

		commitOverrides := graveler.CommitOverrides{
			Metadata: map[string]string{
				"originalKey": "overrideValue",
				"newKey":      "newValue",
			},
		}

		setupSuccessfulRevertExpectations(test)
		test.RefManager.EXPECT().AddCommit(ctx, repository, gomock.Any()).DoAndReturn(func(ctx context.Context, repository *graveler.RepositoryRecord, commit graveler.Commit) (graveler.CommitID, error) {
			require.Equal(t, mr3ID, commit.MetaRangeID)
			require.Equal(t, "original message", commit.Message)
			require.Equal(t, commitOverrides.Metadata, commit.Metadata)
			return commit3ID, nil
		}).Times(1)

		val, err := test.Sut.Revert(ctx, repository, branch1ID, graveler.Ref(commit2ID), 0, commitParams, &commitOverrides)

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, commit3ID, graveler.CommitID(val.Ref()))
	})

	t.Run("revert with nil overrides", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		firstUpdateBranch(test)
		emptyStagingTokenCombo(test, 2)

		commitParams := graveler.CommitParams{
			Message: "original message",
			Metadata: map[string]string{
				"originalKey": "originalValue",
			},
		}

		setupSuccessfulRevertExpectations(test)
		test.RefManager.EXPECT().AddCommit(ctx, repository, gomock.Any()).DoAndReturn(func(ctx context.Context, repository *graveler.RepositoryRecord, commit graveler.Commit) (graveler.CommitID, error) {
			require.Equal(t, mr3ID, commit.MetaRangeID)
			return commit3ID, nil
		}).Times(1)

		val, err := test.Sut.Revert(ctx, repository, branch1ID, graveler.Ref(commit2ID), 0, commitParams, nil)

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, commit3ID, graveler.CommitID(val.Ref()))
	})

	t.Run("revert dirty branch after token update", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		firstUpdateBranch(test)
		emptyStagingTokenCombo(test, 1)
		dirtyStagingTokenCombo(test)

		test.RefManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(2).Return(&commit1, nil)
		test.CommittedManager.EXPECT().List(ctx, repository.StorageNamespace, mr1ID).Times(2).Return(testutils.NewFakeValueIterator(nil), nil)
		test.RefManager.EXPECT().ParseRef(graveler.Ref(commit2ID)).Times(1).Return(rawRefCommit2, nil)
		test.RefManager.EXPECT().ResolveRawRef(ctx, repository, rawRefCommit2).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeCommit, BranchRecord: graveler.BranchRecord{Branch: &graveler.Branch{CommitID: commit2ID}}}, nil)
		test.RefManager.EXPECT().GetCommit(ctx, repository, commit2ID).Times(1).Return(&commit2, nil)
		test.RefManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				branchTest := &graveler.Branch{StagingToken: stagingToken4, CommitID: commit1ID, SealedTokens: []graveler.StagingToken{stagingToken1, stagingToken2, stagingToken3}}
				updatedBranch, err := f(branchTest)
				require.True(t, errors.Is(err, graveler.ErrDirtyBranch))
				require.Nil(t, updatedBranch)
				return err
			}).Times(1)

		val, err := test.Sut.Revert(ctx, repository, branch1ID, graveler.Ref(commit2ID), 0, graveler.CommitParams{}, &graveler.CommitOverrides{})

		require.True(t, errors.Is(err, graveler.ErrDirtyBranch))
		require.Equal(t, "", val.String())
	})
}

func TestGravelerCherryPick(t *testing.T) {
	ctx := context.Background()

	firstUpdateBranch := func(test *testutil.GravelerTest) {
		test.RefManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			Do(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				branchTest := branch1
				updatedBranch, err := f(&branchTest)
				require.NoError(t, err)
				require.Equal(t, []graveler.StagingToken{stagingToken1, stagingToken2, stagingToken3}, updatedBranch.SealedTokens)
				require.NotEmpty(t, updatedBranch.StagingToken)
				require.NotEqual(t, stagingToken1, updatedBranch.StagingToken)
				return nil
			}).Times(1)
	}
	emptyStagingTokenCombo := func(test *testutil.GravelerTest, times int) {
		test.StagingManager.EXPECT().List(ctx, stagingToken1, gomock.Any()).Times(times).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{{
			Key:   key1,
			Value: nil, // tombstone
		}}))
		test.StagingManager.EXPECT().List(ctx, stagingToken2, gomock.Any()).Times(times).Return(testutils.NewFakeValueIterator(nil))
		test.StagingManager.EXPECT().List(ctx, stagingToken3, gomock.Any()).Times(times).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{{
			Key:   key1,
			Value: value1,
		}}))
	}

	setupCherryPickExpectations := func(test *testutil.GravelerTest) {
		test.RefManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(3).Return(&commit1, nil)
		test.CommittedManager.EXPECT().List(ctx, repository.StorageNamespace, mr1ID).Times(2).Return(testutils.NewFakeValueIterator(nil), nil)
		test.RefManager.EXPECT().ParseRef(graveler.Ref(commit2ID)).Times(1).Return(rawRefCommit2, nil)
		test.RefManager.EXPECT().ParseRef(graveler.Ref(commit1ID)).Times(1).Return(rawRefCommit1, nil)
		test.RefManager.EXPECT().ParseRef(graveler.Ref(commit4ID)).Times(1).Return(rawRefCommit4, nil)
		test.RefManager.EXPECT().ResolveRawRef(ctx, repository, rawRefCommit2).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeCommit, BranchRecord: graveler.BranchRecord{Branch: &graveler.Branch{CommitID: commit2ID}}}, nil)
		test.RefManager.EXPECT().ResolveRawRef(ctx, repository, rawRefCommit1).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeCommit, BranchRecord: graveler.BranchRecord{Branch: &graveler.Branch{CommitID: commit1ID}}}, nil)
		test.RefManager.EXPECT().ResolveRawRef(ctx, repository, rawRefCommit4).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeCommit, BranchRecord: graveler.BranchRecord{Branch: &graveler.Branch{CommitID: commit4ID}}}, nil)
		test.RefManager.EXPECT().GetCommit(ctx, repository, commit2ID).Times(1).Return(&commit2, nil)
		test.RefManager.EXPECT().GetCommit(ctx, repository, commit4ID).Times(1).Return(&commit4, nil)
		test.CommittedManager.EXPECT().Merge(ctx, repository.StorageNamespace, mr1ID, mr2ID, mr4ID, graveler.MergeStrategyNone, []graveler.SetOptionsFunc{}).Times(1).Return(mr3ID, nil)
		test.RefManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			Do(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				branchTest := &graveler.Branch{StagingToken: stagingToken4, CommitID: commit1ID, SealedTokens: []graveler.StagingToken{stagingToken1, stagingToken2, stagingToken3}}
				updatedBranch, err := f(branchTest)
				require.NoError(t, err)
				require.Equal(t, []graveler.StagingToken{}, updatedBranch.SealedTokens)
				require.NotEmpty(t, updatedBranch.StagingToken)
				require.Equal(t, commit3ID, updatedBranch.CommitID)
				return nil
			}).Times(1)
		test.StagingManager.EXPECT().DropAsync(ctx, stagingToken1).Times(1)
		test.StagingManager.EXPECT().DropAsync(ctx, stagingToken2).Times(1)
		test.StagingManager.EXPECT().DropAsync(ctx, stagingToken3).Times(1)
	}
	t.Run("cherry-pick successful", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		firstUpdateBranch(test)
		emptyStagingTokenCombo(test, 2)

		setupCherryPickExpectations(test)
		test.RefManager.EXPECT().AddCommit(ctx, repository, gomock.Any()).DoAndReturn(func(ctx context.Context, repository *graveler.RepositoryRecord, commit graveler.Commit) (graveler.CommitID, error) {
			require.Equal(t, mr3ID, commit.MetaRangeID)
			return commit3ID, nil
		}).Times(1)

		parent := 1
		val, err := test.Sut.CherryPick(ctx, repository, branch1ID, graveler.Ref(commit2ID), &parent, "tester", &graveler.CommitOverrides{})

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, commit3ID, graveler.CommitID(val.Ref()))
	})

	t.Run("cherry-pick override commit fields", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		firstUpdateBranch(test)
		emptyStagingTokenCombo(test, 2)
		commitOverrides := graveler.CommitOverrides{
			Message: "override message",
			Metadata: map[string]string{
				"overrideKey": "overrideValue",
			},
		}

		setupCherryPickExpectations(test)
		test.RefManager.EXPECT().AddCommit(ctx, repository, gomock.Any()).DoAndReturn(func(ctx context.Context, repository *graveler.RepositoryRecord, commit graveler.Commit) (graveler.CommitID, error) {
			require.Equal(t, mr3ID, commit.MetaRangeID)
			require.Equal(t, commitOverrides.Message, commit.Message)
			for k, v := range commitOverrides.Metadata {
				require.Equal(t, v, commit.Metadata[k])
			}
			return commit3ID, nil
		}).Times(1)
		parent := 1
		val, err := test.Sut.CherryPick(ctx, repository, branch1ID, graveler.Ref(commit2ID), &parent, "tester", &commitOverrides)

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, commit3ID, graveler.CommitID(val.Ref()))
	})

	t.Run("cherry-pick partially override commit fields", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		firstUpdateBranch(test)
		emptyStagingTokenCombo(test, 2)
		commitOverrides := graveler.CommitOverrides{
			Metadata: map[string]string{
				"overrideKey": "overrideValue",
			},
		}

		setupCherryPickExpectations(test)
		test.RefManager.EXPECT().AddCommit(ctx, repository, gomock.Any()).DoAndReturn(func(ctx context.Context, repository *graveler.RepositoryRecord, commit graveler.Commit) (graveler.CommitID, error) {
			require.Equal(t, mr3ID, commit.MetaRangeID)
			require.Equal(t, "", commit.Message)
			require.Equal(t, commitOverrides.Metadata, commit.Metadata)
			return commit3ID, nil
		}).Times(1)
		parent := 1
		val, err := test.Sut.CherryPick(ctx, repository, branch1ID, graveler.Ref(commit2ID), &parent, "tester", &commitOverrides)

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, commit3ID, graveler.CommitID(val.Ref()))
	})

	t.Run("cherry-pick with nil overrides", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		firstUpdateBranch(test)
		emptyStagingTokenCombo(test, 2)

		setupCherryPickExpectations(test)
		test.RefManager.EXPECT().AddCommit(ctx, repository, gomock.Any()).DoAndReturn(func(ctx context.Context, repository *graveler.RepositoryRecord, commit graveler.Commit) (graveler.CommitID, error) {
			require.Equal(t, mr3ID, commit.MetaRangeID)
			return commit3ID, nil
		}).Times(1)
		parent := 1
		val, err := test.Sut.CherryPick(ctx, repository, branch1ID, graveler.Ref(commit2ID), &parent, "tester", nil)

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, commit3ID, graveler.CommitID(val.Ref()))
	})
}

func TestGravelerCommit_v2(t *testing.T) {
	ctx := context.Background()

	t.Run("commit with sealed tokens", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		var updatedSealedBranch graveler.Branch
		test.ProtectedBranchesManager.EXPECT().IsBlocked(ctx, repository, branch1ID, graveler.BranchProtectionBlockedAction_COMMIT).Return(false, nil)

		test.RefManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			Do(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				branchTest := branch1
				updatedBranch, err := f(&branchTest)
				updatedSealedBranch = *updatedBranch
				require.NoError(t, err)
				require.Equal(t, []graveler.StagingToken{stagingToken1, stagingToken2, stagingToken3}, updatedBranch.SealedTokens)
				require.NotEmpty(t, updatedBranch.StagingToken)
				require.NotEqual(t, stagingToken1, updatedBranch.StagingToken)
				return nil
			}).Times(1)

		test.RefManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			Do(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				updatedBranch, err := f(&updatedSealedBranch)
				require.NoError(t, err)
				require.Equal(t, []graveler.StagingToken{}, updatedBranch.SealedTokens)
				require.NotEqual(t, commit1ID, updatedBranch.CommitID)
				return nil
			}).Times(1)

		test.RefManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(1).Return(&commit1, nil)
		test.StagingManager.EXPECT().List(ctx, stagingToken1, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{}))
		test.StagingManager.EXPECT().List(ctx, stagingToken2, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{}))
		test.StagingManager.EXPECT().List(ctx, stagingToken3, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{}))
		test.CommittedManager.EXPECT().Commit(ctx, repository.StorageNamespace, mr1ID, gomock.Any(), false, []graveler.SetOptionsFunc{}).Times(1).Return(graveler.MetaRangeID(""), graveler.DiffSummary{}, nil)
		test.RefManager.EXPECT().AddCommit(ctx, repository, gomock.Any()).Return(graveler.CommitID(""), nil)
		test.StagingManager.EXPECT().DropAsync(ctx, stagingToken1).Return(nil)
		test.StagingManager.EXPECT().DropAsync(ctx, stagingToken2).Return(nil)
		test.StagingManager.EXPECT().DropAsync(ctx, stagingToken3).Return(nil)

		val, err := test.Sut.Commit(ctx, repository, branch1ID, graveler.CommitParams{})

		require.NoError(t, err)
		require.NotNil(t, val)
	})

	t.Run("commit no changes", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		var updatedSealedBranch graveler.Branch
		test.ProtectedBranchesManager.EXPECT().IsBlocked(ctx, repository, branch1ID, graveler.BranchProtectionBlockedAction_COMMIT).Return(false, nil)

		test.RefManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			Do(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				branchTest := branch1
				updatedBranch, err := f(&branchTest)
				updatedSealedBranch = *updatedBranch
				require.NoError(t, err)
				require.Equal(t, []graveler.StagingToken{stagingToken1, stagingToken2, stagingToken3}, updatedBranch.SealedTokens)
				require.NotEmpty(t, updatedBranch.StagingToken)
				require.NotEqual(t, stagingToken1, updatedBranch.StagingToken)
				return nil
			}).Times(1)

		test.RefManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			Do(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				updatedBranch, err := f(&updatedSealedBranch)
				require.Error(t, err)
				require.True(t, errors.Is(err, graveler.ErrNoChanges))
				require.Nil(t, updatedBranch)
				return err
			}).Times(1).Return(graveler.ErrNoChanges)

		test.RefManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(1).Return(&commit1, nil)
		test.StagingManager.EXPECT().List(ctx, stagingToken1, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{}))
		test.StagingManager.EXPECT().List(ctx, stagingToken2, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{}))
		test.StagingManager.EXPECT().List(ctx, stagingToken3, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{}))
		test.CommittedManager.EXPECT().Commit(ctx, repository.StorageNamespace, mr1ID, gomock.Any(), false, []graveler.SetOptionsFunc{}).Times(1).Return(graveler.MetaRangeID(""), graveler.DiffSummary{}, graveler.ErrNoChanges)

		val, err := test.Sut.Commit(ctx, repository, branch1ID, graveler.CommitParams{})

		require.Error(t, err)
		require.True(t, errors.Is(err, graveler.ErrNoChanges))
		require.Equal(t, val, graveler.CommitID(""))
	})

	t.Run("commit failed retryUpdateBranch", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		test.ProtectedBranchesManager.EXPECT().IsBlocked(ctx, repository, branch1ID, graveler.BranchProtectionBlockedAction_COMMIT).Return(false, nil)

		test.RefManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			Do(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				branchTest := branch1
				updatedBranch, err := f(&branchTest)
				require.NoError(t, err)
				require.Equal(t, []graveler.StagingToken{stagingToken1, stagingToken2, stagingToken3}, updatedBranch.SealedTokens)
				require.NotEmpty(t, updatedBranch.StagingToken)
				require.NotEqual(t, stagingToken1, updatedBranch.StagingToken)
				return nil
			}).Times(1)

		test.RefManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).Times(graveler.BranchUpdateMaxTries).Return(kv.ErrPredicateFailed)

		val, err := test.Sut.Commit(ctx, repository, branch1ID, graveler.CommitParams{})

		require.Error(t, err)
		require.True(t, errors.Is(err, graveler.ErrTooManyTries))
		require.Equal(t, val, graveler.CommitID(""))
	})
}

func TestGravelerCreateCommitRecord_v2(t *testing.T) {
	ctx := context.Background()
	t.Run("create commit record", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		commit := graveler.Commit{
			Committer:    "committer",
			Message:      "message",
			MetaRangeID:  "metaRangeID",
			Parents:      []graveler.CommitID{"parent1", "parent2"},
			Metadata:     graveler.Metadata{"key": "value"},
			CreationDate: time.Now(),
			Version:      graveler.CurrentCommitVersion,
			Generation:   1,
		}
		test.RefManager.EXPECT().CreateCommitRecord(ctx, repository, graveler.CommitID("commitID"), commit).Return(nil)
		err := test.Sut.CreateCommitRecord(ctx, repository, "commitID", commit)
		require.NoError(t, err)
	})
}

func TestGravelerImport(t *testing.T) {
	ctx := context.Background()

	firstUpdateBranch := func(test *testutil.GravelerTest) {
		test.RefManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			Do(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				branchTest := branch1
				updatedBranch, err := f(&branchTest)
				require.NoError(t, err)
				require.Equal(t, []graveler.StagingToken{stagingToken1, stagingToken2, stagingToken3}, updatedBranch.SealedTokens)
				require.NotEmpty(t, updatedBranch.StagingToken)
				require.NotEqual(t, stagingToken1, updatedBranch.StagingToken)
				return nil
			}).Times(1)
	}
	emptyStagingTokenCombo := func(test *testutil.GravelerTest, numReps int) {
		test.StagingManager.EXPECT().List(ctx, stagingToken1, gomock.Any()).Times(numReps).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{{
			Key:   key1,
			Value: nil, // tombstone
		}}))
		test.StagingManager.EXPECT().List(ctx, stagingToken2, gomock.Any()).Times(numReps).Return(testutils.NewFakeValueIterator(nil))
		test.StagingManager.EXPECT().List(ctx, stagingToken3, gomock.Any()).Times(numReps).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{{
			Key:   key1,
			Value: value1,
		}}))
	}

	importTest := func(t *testing.T, metadata graveler.Metadata) {
		test := testutil.InitGravelerTest(t)
		firstUpdateBranch(test)
		emptyStagingTokenCombo(test, 2)
		test.RefManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(3).Return(&commit1, nil)
		test.CommittedManager.EXPECT().List(ctx, repository.StorageNamespace, mr1ID).Times(2).Return(testutils.NewFakeValueIterator(nil), nil)
		test.RefManager.EXPECT().ParseRef(graveler.Ref(branch1ID)).Times(1).Return(rawRefCommit1, nil)
		test.RefManager.EXPECT().ResolveRawRef(ctx, repository, rawRefCommit1).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeCommit, BranchRecord: graveler.BranchRecord{Branch: &graveler.Branch{CommitID: commit1ID}}}, nil)
		test.CommittedManager.EXPECT().Import(ctx, repository.StorageNamespace, mr1ID, mr2ID, nil, []graveler.SetOptionsFunc{}).Times(1).Return(mr4ID, nil)
		test.RefManager.EXPECT().AddCommit(ctx, repository, gomock.Any()).DoAndReturn(func(ctx context.Context, repository *graveler.RepositoryRecord, commit graveler.Commit) (graveler.CommitID, error) {
			require.Equal(t, mr4ID, commit.MetaRangeID)
			return commit4ID, nil
		}).Times(1)
		test.RefManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			Do(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				branchTest := &graveler.Branch{StagingToken: stagingToken4, CommitID: commit1ID, SealedTokens: []graveler.StagingToken{stagingToken1, stagingToken2, stagingToken3}}
				updatedBranch, err := f(branchTest)
				require.NoError(t, err)
				require.Equal(t, []graveler.StagingToken{}, updatedBranch.SealedTokens)
				require.NotEmpty(t, updatedBranch.StagingToken)
				require.Equal(t, commit4ID, updatedBranch.CommitID)
				return nil
			}).Times(1)
		test.StagingManager.EXPECT().DropAsync(ctx, stagingToken1).Times(1)
		test.StagingManager.EXPECT().DropAsync(ctx, stagingToken2).Times(1)
		test.StagingManager.EXPECT().DropAsync(ctx, stagingToken3).Times(1)
		test.RefManager.EXPECT().SetRepositoryMetadata(ctx, repository, gomock.Any())

		val, err := test.Sut.Import(ctx, repository, branch1ID, commit2.MetaRangeID, graveler.CommitParams{Metadata: metadata}, nil)
		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, commit4ID, graveler.CommitID(val.Ref()))
	}

	t.Run("import successful without metadata", func(t *testing.T) {
		importTest(t, nil)
	})

	t.Run("import successful with metadata", func(t *testing.T) {
		importTest(t, graveler.Metadata{"key": "value"})
	})
}
