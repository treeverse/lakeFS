package graveler_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/kv"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/catalog/testutils"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/mock"
)

type gravelerTest struct {
	db                       *graveler.DBGraveler
	hooks                    graveler.HooksHandler
	committedManager         *mock.MockCommittedManager
	refManager               *mock.MockRefManager
	stagingManager           *mock.MockStagingManager
	protectedBranchesManager *mock.MockProtectedBranchesManager
	garbageCollectionManager *mock.MockGarbageCollectionManager
	sut                      *graveler.KVGraveler
}

func initGravelerTest(t *testing.T) *gravelerTest {
	ctrl := gomock.NewController(t)

	test := &gravelerTest{
		committedManager:         mock.NewMockCommittedManager(ctrl),
		stagingManager:           mock.NewMockStagingManager(ctrl),
		refManager:               mock.NewMockRefManager(ctrl),
		garbageCollectionManager: mock.NewMockGarbageCollectionManager(ctrl),
		protectedBranchesManager: mock.NewMockProtectedBranchesManager(ctrl),
	}

	test.sut = graveler.NewKVGraveler(test.committedManager, test.stagingManager, test.refManager, test.garbageCollectionManager, test.protectedBranchesManager)

	return test
}

var (
	repoID        = graveler.RepositoryID("repo1")
	branch1ID     = graveler.BranchID("branch1")
	branch2ID     = graveler.BranchID("branch2")
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
	branch1 = graveler.Branch{
		CommitID:     commit1ID,
		StagingToken: stagingToken1,
		SealedTokens: []graveler.StagingToken{stagingToken2, stagingToken3},
	}
	commit1       = graveler.Commit{MetaRangeID: mr1ID, Parents: []graveler.CommitID{commit4ID}}
	commit2       = graveler.Commit{MetaRangeID: mr2ID, Parents: []graveler.CommitID{commit4ID}}
	commit3       = graveler.Commit{MetaRangeID: mr3ID}
	commit4       = graveler.Commit{MetaRangeID: mr4ID}
	rawRefBranch  = graveler.RawRef{BaseRef: string(branch1ID)}
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
	setupGetFromBranch := func(test *gravelerTest) {
		test.refManager.EXPECT().ParseRef(graveler.Ref(branch1ID)).Times(1).Return(rawRefBranch, nil)
		test.refManager.EXPECT().ResolveRawRef(ctx, repository, rawRefBranch).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeBranch, BranchRecord: graveler.BranchRecord{BranchID: branch1ID, Branch: &branch1}}, nil)
	}

	setupGetFromCommit := func(test *gravelerTest) {
		test.refManager.EXPECT().ParseRef(graveler.Ref(commit1ID)).Times(1).Return(rawRefCommit1, nil)
		test.refManager.EXPECT().ResolveRawRef(ctx, repository, rawRefCommit1).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeCommit, BranchRecord: graveler.BranchRecord{Branch: &graveler.Branch{CommitID: commit1ID}}}, nil)
	}

	t.Run("get from branch - staging", func(t *testing.T) {
		test := initGravelerTest(t)
		setupGetFromBranch(test)

		test.stagingManager.EXPECT().Get(ctx, stagingToken1, key1).Times(1).Return(value1, nil)

		val, err := test.sut.Get(ctx, repository, graveler.Ref(branch1ID), key1)

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, value1, val)
	})

	t.Run("get from branch - commit", func(t *testing.T) {
		test := initGravelerTest(t)
		setupGetFromBranch(test)

		test.stagingManager.EXPECT().Get(ctx, stagingToken1, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.stagingManager.EXPECT().Get(ctx, stagingToken2, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.stagingManager.EXPECT().Get(ctx, stagingToken3, key1).Times(1).Return(nil, graveler.ErrNotFound)

		test.refManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(1).Return(&commit1, nil)
		test.committedManager.EXPECT().Get(ctx, repository.StorageNamespace, commit1.MetaRangeID, key1).Times(1).Return(value1, nil)

		val, err := test.sut.Get(ctx, repository, graveler.Ref(branch1ID), key1)

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, value1, val)
	})

	t.Run("get from branch - staging tombstone", func(t *testing.T) {
		test := initGravelerTest(t)
		setupGetFromBranch(test)

		test.stagingManager.EXPECT().Get(ctx, stagingToken1, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.stagingManager.EXPECT().Get(ctx, stagingToken2, key1).Times(1).Return(nil, nil)

		val, err := test.sut.Get(ctx, repository, graveler.Ref(branch1ID), key1)

		require.Error(t, graveler.ErrNotFound, err)
		require.Nil(t, val)
	})

	t.Run("get from branch - not found", func(t *testing.T) {
		test := initGravelerTest(t)
		setupGetFromBranch(test)

		test.stagingManager.EXPECT().Get(ctx, stagingToken1, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.stagingManager.EXPECT().Get(ctx, stagingToken2, key1).Times(1).Return(nil, graveler.ErrNotFound)
		test.stagingManager.EXPECT().Get(ctx, stagingToken3, key1).Times(1).Return(nil, graveler.ErrNotFound)

		test.refManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(1).Return(&commit1, nil)
		test.committedManager.EXPECT().Get(ctx, repository.StorageNamespace, commit1.MetaRangeID, key1).Times(1).Return(nil, graveler.ErrNotFound)

		val, err := test.sut.Get(ctx, repository, graveler.Ref(branch1ID), key1)

		require.Error(t, graveler.ErrNotFound, err)
		require.Nil(t, val)
	})

	t.Run("get from commit", func(t *testing.T) {
		test := initGravelerTest(t)
		setupGetFromCommit(test)

		test.refManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(1).Return(&commit1, nil)
		test.committedManager.EXPECT().Get(ctx, repository.StorageNamespace, commit1.MetaRangeID, key1).Times(1).Return(value1, nil)

		val, err := test.sut.Get(ctx, repository, graveler.Ref(commit1ID), key1)

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, value1, val)
	})

	t.Run("get from commit - not found", func(t *testing.T) {
		test := initGravelerTest(t)
		setupGetFromCommit(test)

		test.refManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(1).Return(&commit1, nil)
		test.committedManager.EXPECT().Get(ctx, repository.StorageNamespace, commit1.MetaRangeID, key1).Times(1).Return(nil, graveler.ErrNotFound)

		val, err := test.sut.Get(ctx, repository, graveler.Ref(commit1ID), key1)

		require.Error(t, graveler.ErrNotFound, err)
		require.Nil(t, val)
	})
}

func TestGravelerMerge(t *testing.T) {
	ctx := context.Background()

	firstUpdateBranch := func(test *gravelerTest) {
		test.refManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
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
	emptyStagingTokenCombo := func(test *gravelerTest) {
		test.stagingManager.EXPECT().List(ctx, stagingToken1, gomock.Any()).Times(2).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{{
			Key:   key1,
			Value: nil, // tombstone
		}}), nil)
		test.stagingManager.EXPECT().List(ctx, stagingToken2, gomock.Any()).Times(2).Return(testutils.NewFakeValueIterator(nil), nil)
		test.stagingManager.EXPECT().List(ctx, stagingToken3, gomock.Any()).Times(2).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{{
			Key:   key1,
			Value: value1,
		}}), nil)
	}
	t.Run("merge successful", func(t *testing.T) {
		test := initGravelerTest(t)
		firstUpdateBranch(test)
		emptyStagingTokenCombo(test)
		test.refManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(3).Return(&commit1, nil)
		test.committedManager.EXPECT().List(ctx, repository.StorageNamespace, mr1ID).Times(2).Return(testutils.NewFakeValueIterator(nil), nil)
		test.refManager.EXPECT().ParseRef(graveler.Ref(branch2ID)).Times(1).Return(rawRefCommit2, nil)
		test.refManager.EXPECT().ParseRef(graveler.Ref(branch1ID)).Times(1).Return(rawRefCommit1, nil)
		test.refManager.EXPECT().ResolveRawRef(ctx, repository, rawRefCommit2).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeCommit, BranchRecord: graveler.BranchRecord{Branch: &graveler.Branch{CommitID: commit2ID}}}, nil)
		test.refManager.EXPECT().ResolveRawRef(ctx, repository, rawRefCommit1).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeCommit, BranchRecord: graveler.BranchRecord{Branch: &graveler.Branch{CommitID: commit1ID}}}, nil)
		test.refManager.EXPECT().GetCommit(ctx, repository, commit2ID).Times(1).Return(&commit2, nil)
		test.refManager.EXPECT().FindMergeBase(ctx, repository, commit2ID, commit1ID).Times(1).Return(&commit3, nil)
		test.committedManager.EXPECT().Merge(ctx, repository.StorageNamespace, mr1ID, mr2ID, mr3ID, graveler.MergeStrategyNone).Times(1).Return(mr4ID, nil)
		test.refManager.EXPECT().AddCommit(ctx, repository, gomock.Any()).DoAndReturn(func(ctx context.Context, repository *graveler.RepositoryRecord, commit graveler.Commit) (graveler.CommitID, error) {
			require.Equal(t, mr4ID, commit.MetaRangeID)
			return commit4ID, nil
		}).Times(1)
		test.refManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			Do(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				branchTest := &graveler.Branch{StagingToken: stagingToken4, CommitID: commit1ID, SealedTokens: []graveler.StagingToken{stagingToken1, stagingToken2, stagingToken3}}
				updatedBranch, err := f(branchTest)
				require.NoError(t, err)
				require.Equal(t, []graveler.StagingToken{}, updatedBranch.SealedTokens)
				require.NotEmpty(t, updatedBranch.StagingToken)
				require.Equal(t, commit4ID, updatedBranch.CommitID)
				return nil
			}).Times(1)
		test.stagingManager.EXPECT().DropAsync(ctx, stagingToken1).Times(1)
		test.stagingManager.EXPECT().DropAsync(ctx, stagingToken2).Times(1)
		test.stagingManager.EXPECT().DropAsync(ctx, stagingToken3).Times(1)

		val, err := test.sut.Merge(ctx, repository, branch1ID, graveler.Ref(branch2ID), graveler.CommitParams{}, "")

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, commit4ID, graveler.CommitID(val.Ref()))
	})

	t.Run("merge dirty destination while updating tokens", func(t *testing.T) {
		test := initGravelerTest(t)
		test.refManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				branchTest := branch1
				updatedBranch, err := f(&branchTest)
				require.Error(t, err)
				require.Nil(t, updatedBranch)
				return err
			}).Times(1)

		test.stagingManager.EXPECT().List(ctx, stagingToken1, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{{
			Key:   key1,
			Value: nil, // tombstone
		}}), nil)
		test.stagingManager.EXPECT().List(ctx, stagingToken2, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{{
			Key:   key2,
			Value: value2,
		}}), nil)
		test.stagingManager.EXPECT().List(ctx, stagingToken3, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{{
			Key:   key1,
			Value: value1,
		}}), nil)
		test.refManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(1).Return(&commit1, nil)
		test.committedManager.EXPECT().List(ctx, repository.StorageNamespace, mr1ID).Times(1).Return(testutils.NewFakeValueIterator(nil), nil)

		val, err := test.sut.Merge(ctx, repository, branch1ID, graveler.Ref(branch2ID), graveler.CommitParams{}, "")

		require.Equal(t, graveler.ErrConflictFound, err)
		require.Equal(t, graveler.CommitID(""), val)
	})
}

func TestGravelerRevert(t *testing.T) {
	ctx := context.Background()

	firstUpdateBranch := func(test *gravelerTest) {
		test.refManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
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
	emptyStagingTokenCombo := func(test *gravelerTest, times int) {
		test.stagingManager.EXPECT().List(ctx, stagingToken1, gomock.Any()).Times(times).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{{
			Key:   key1,
			Value: nil, // tombstone
		}}), nil)
		test.stagingManager.EXPECT().List(ctx, stagingToken2, gomock.Any()).Times(times).Return(testutils.NewFakeValueIterator(nil), nil)
		test.stagingManager.EXPECT().List(ctx, stagingToken3, gomock.Any()).Times(times).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{{
			Key:   key1,
			Value: value1,
		}}), nil)
	}
	dirtyStagingTokenCombo := func(test *gravelerTest) {
		test.stagingManager.EXPECT().List(ctx, stagingToken1, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator(nil), nil)
		test.stagingManager.EXPECT().List(ctx, stagingToken2, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator(nil), nil)
		test.stagingManager.EXPECT().List(ctx, stagingToken3, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{{
			Key:   key1,
			Value: value1,
		}}), nil)
	}
	t.Run("revert successful", func(t *testing.T) {
		test := initGravelerTest(t)
		firstUpdateBranch(test)
		emptyStagingTokenCombo(test, 2)
		test.refManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(3).Return(&commit1, nil)
		test.committedManager.EXPECT().List(ctx, repository.StorageNamespace, mr1ID).Times(2).Return(testutils.NewFakeValueIterator(nil), nil)
		test.refManager.EXPECT().ParseRef(graveler.Ref(commit2ID)).Times(1).Return(rawRefCommit2, nil)
		test.refManager.EXPECT().ParseRef(graveler.Ref(commit1ID)).Times(1).Return(rawRefCommit1, nil)
		test.refManager.EXPECT().ParseRef(graveler.Ref(commit4ID)).Times(1).Return(rawRefCommit4, nil)
		test.refManager.EXPECT().ResolveRawRef(ctx, repository, rawRefCommit2).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeCommit, BranchRecord: graveler.BranchRecord{Branch: &graveler.Branch{CommitID: commit2ID}}}, nil)
		test.refManager.EXPECT().ResolveRawRef(ctx, repository, rawRefCommit1).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeCommit, BranchRecord: graveler.BranchRecord{Branch: &graveler.Branch{CommitID: commit1ID}}}, nil)
		test.refManager.EXPECT().ResolveRawRef(ctx, repository, rawRefCommit4).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeCommit, BranchRecord: graveler.BranchRecord{Branch: &graveler.Branch{CommitID: commit4ID}}}, nil)
		test.refManager.EXPECT().GetCommit(ctx, repository, commit2ID).Times(1).Return(&commit2, nil)
		test.refManager.EXPECT().GetCommit(ctx, repository, commit4ID).Times(1).Return(&commit4, nil)
		test.committedManager.EXPECT().Merge(ctx, repository.StorageNamespace, mr1ID, mr4ID, mr2ID, graveler.MergeStrategyNone).Times(1).Return(mr3ID, nil)
		test.refManager.EXPECT().AddCommit(ctx, repository, gomock.Any()).DoAndReturn(func(ctx context.Context, repository *graveler.RepositoryRecord, commit graveler.Commit) (graveler.CommitID, error) {
			require.Equal(t, mr3ID, commit.MetaRangeID)
			return commit3ID, nil
		}).Times(1)
		test.refManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			Do(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				branchTest := &graveler.Branch{StagingToken: stagingToken4, CommitID: commit1ID, SealedTokens: []graveler.StagingToken{stagingToken1, stagingToken2, stagingToken3}}
				updatedBranch, err := f(branchTest)
				require.NoError(t, err)
				require.Equal(t, []graveler.StagingToken{}, updatedBranch.SealedTokens)
				require.NotEmpty(t, updatedBranch.StagingToken)
				require.Equal(t, commit3ID, updatedBranch.CommitID)
				return nil
			}).Times(1)
		test.stagingManager.EXPECT().DropAsync(ctx, stagingToken1).Times(1)
		test.stagingManager.EXPECT().DropAsync(ctx, stagingToken2).Times(1)
		test.stagingManager.EXPECT().DropAsync(ctx, stagingToken3).Times(1)

		val, err := test.sut.Revert(ctx, repository, branch1ID, graveler.Ref(commit2ID), 0, graveler.CommitParams{})

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, commit3ID, graveler.CommitID(val.Ref()))
	})

	t.Run("revert dirty branch after token update", func(t *testing.T) {
		test := initGravelerTest(t)
		firstUpdateBranch(test)
		emptyStagingTokenCombo(test, 1)
		dirtyStagingTokenCombo(test)

		test.refManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(2).Return(&commit1, nil)
		test.committedManager.EXPECT().List(ctx, repository.StorageNamespace, mr1ID).Times(2).Return(testutils.NewFakeValueIterator(nil), nil)
		test.refManager.EXPECT().ParseRef(graveler.Ref(commit2ID)).Times(1).Return(rawRefCommit2, nil)
		test.refManager.EXPECT().ResolveRawRef(ctx, repository, rawRefCommit2).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeCommit, BranchRecord: graveler.BranchRecord{Branch: &graveler.Branch{CommitID: commit2ID}}}, nil)
		test.refManager.EXPECT().GetCommit(ctx, repository, commit2ID).Times(1).Return(&commit2, nil)
		test.refManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				branchTest := &graveler.Branch{StagingToken: stagingToken4, CommitID: commit1ID, SealedTokens: []graveler.StagingToken{stagingToken1, stagingToken2, stagingToken3}}
				updatedBranch, err := f(branchTest)
				require.True(t, errors.Is(err, graveler.ErrDirtyBranch))
				require.Nil(t, updatedBranch)
				return err
			}).Times(1)

		val, err := test.sut.Revert(ctx, repository, branch1ID, graveler.Ref(commit2ID), 0, graveler.CommitParams{})

		require.True(t, errors.Is(err, graveler.ErrDirtyBranch))
		require.Equal(t, "", val.String())
	})
}

func TestGravelerCommit(t *testing.T) {
	ctx := context.Background()

	t.Run("commit with sealed tokens", func(t *testing.T) {
		test := initGravelerTest(t)
		var updatedSealedBranch graveler.Branch
		test.protectedBranchesManager.EXPECT().IsBlocked(ctx, repository, branch1ID, graveler.BranchProtectionBlockedAction_COMMIT).Return(false, nil)

		test.refManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
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

		test.refManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			Do(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				updatedBranch, err := f(&updatedSealedBranch)
				require.NoError(t, err)
				require.Equal(t, []graveler.StagingToken{}, updatedBranch.SealedTokens)
				require.NotEqual(t, commit1ID, updatedBranch.CommitID)
				return nil
			}).Times(1)

		test.refManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(1).Return(&commit1, nil)
		test.stagingManager.EXPECT().List(ctx, stagingToken1, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{}), nil)
		test.stagingManager.EXPECT().List(ctx, stagingToken2, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{}), nil)
		test.stagingManager.EXPECT().List(ctx, stagingToken3, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{}), nil)
		test.committedManager.EXPECT().Commit(ctx, repository.StorageNamespace, mr1ID, gomock.Any()).Times(1).Return(graveler.MetaRangeID(""), graveler.DiffSummary{}, nil)
		test.refManager.EXPECT().AddCommit(ctx, repository, gomock.Any()).Return(graveler.CommitID(""), nil)
		test.stagingManager.EXPECT().DropAsync(ctx, stagingToken1).Return(nil)
		test.stagingManager.EXPECT().DropAsync(ctx, stagingToken2).Return(nil)
		test.stagingManager.EXPECT().DropAsync(ctx, stagingToken3).Return(nil)

		val, err := test.sut.Commit(ctx, repository, branch1ID, graveler.CommitParams{})

		require.NoError(t, err)
		require.NotNil(t, val)
	})

	t.Run("commit no changes", func(t *testing.T) {
		test := initGravelerTest(t)
		var updatedSealedBranch graveler.Branch
		test.protectedBranchesManager.EXPECT().IsBlocked(ctx, repository, branch1ID, graveler.BranchProtectionBlockedAction_COMMIT).Return(false, nil)

		test.refManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
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

		test.refManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			Do(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				updatedBranch, err := f(&updatedSealedBranch)
				require.Error(t, err)
				require.True(t, errors.Is(err, graveler.ErrNoChanges))
				require.Nil(t, updatedBranch)
				return err
			}).Times(1).Return(graveler.ErrNoChanges)

		test.refManager.EXPECT().GetCommit(ctx, repository, commit1ID).Times(1).Return(&commit1, nil)
		test.stagingManager.EXPECT().List(ctx, stagingToken1, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{}), nil)
		test.stagingManager.EXPECT().List(ctx, stagingToken2, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{}), nil)
		test.stagingManager.EXPECT().List(ctx, stagingToken3, gomock.Any()).Times(1).Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{}), nil)
		test.committedManager.EXPECT().Commit(ctx, repository.StorageNamespace, mr1ID, gomock.Any()).Times(1).Return(graveler.MetaRangeID(""), graveler.DiffSummary{}, graveler.ErrNoChanges)

		val, err := test.sut.Commit(ctx, repository, branch1ID, graveler.CommitParams{})

		require.Error(t, err)
		require.True(t, errors.Is(err, graveler.ErrNoChanges))
		require.Equal(t, val, graveler.CommitID(""))
	})

	t.Run("commit failed retryUpdateBranch", func(t *testing.T) {
		test := initGravelerTest(t)
		test.protectedBranchesManager.EXPECT().IsBlocked(ctx, repository, branch1ID, graveler.BranchProtectionBlockedAction_COMMIT).Return(false, nil)

		test.refManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).
			Do(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.BranchID, f graveler.BranchUpdateFunc) error {
				branchTest := branch1
				updatedBranch, err := f(&branchTest)
				require.NoError(t, err)
				require.Equal(t, []graveler.StagingToken{stagingToken1, stagingToken2, stagingToken3}, updatedBranch.SealedTokens)
				require.NotEmpty(t, updatedBranch.StagingToken)
				require.NotEqual(t, stagingToken1, updatedBranch.StagingToken)
				return nil
			}).Times(1)

		test.refManager.EXPECT().BranchUpdate(ctx, repository, branch1ID, gomock.Any()).Times(3).Return(kv.ErrPredicateFailed)

		val, err := test.sut.Commit(ctx, repository, branch1ID, graveler.CommitParams{})

		require.Error(t, err)
		require.True(t, errors.Is(err, graveler.ErrTooManyTries))
		require.Equal(t, val, graveler.CommitID(""))
	})
}
