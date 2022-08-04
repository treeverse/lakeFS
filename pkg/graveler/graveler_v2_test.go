package graveler_test

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
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

func TestGravelerGet(t *testing.T) {
	var (
		repoID        = graveler.RepositoryID("repo1")
		branchID      = graveler.BranchID("branch1")
		commitID      = graveler.CommitID("commit1")
		stagingToken1 = graveler.StagingToken("st1")
		stagingToken2 = graveler.StagingToken("st2")
		stagingToken3 = graveler.StagingToken("st3")
		mrID          = graveler.MetaRangeID("mr1")
		repo          = graveler.Repository{
			StorageNamespace: "mock-sn",
			CreationDate:     time.Now(),
			DefaultBranchID:  branchID,
		}
		branch = graveler.Branch{
			CommitID:     commitID,
			StagingToken: stagingToken1,
			SealedTokens: []graveler.StagingToken{stagingToken2, stagingToken3},
		}
		commit = graveler.Commit{
			MetaRangeID: mrID,
		}
		rawRefBranch = graveler.RawRef{BaseRef: string(branchID)}
		rawRefCommit = graveler.RawRef{BaseRef: string(commitID)}
		key          = []byte("some/key")
		value        = &graveler.Value{
			Identity: []byte("id"),
			Data:     []byte("data"),
		}
		ctx = context.Background()
	)

	setupGetFromBranch := func(test *gravelerTest) {
		test.refManager.EXPECT().GetRepository(ctx, repoID).Times(1).Return(&repo, nil)
		test.refManager.EXPECT().ParseRef(graveler.Ref(branchID)).Times(1).Return(rawRefBranch, nil)
		test.refManager.EXPECT().ResolveRawRef(ctx, repoID, rawRefBranch).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeBranch, BranchRecord: graveler.BranchRecord{BranchID: branchID, Branch: &branch}}, nil)
	}

	setupGetFromCommit := func(test *gravelerTest) {
		test.refManager.EXPECT().GetRepository(ctx, repoID).Times(1).Return(&repo, nil)
		test.refManager.EXPECT().ParseRef(graveler.Ref(commitID)).Times(1).Return(rawRefCommit, nil)
		test.refManager.EXPECT().ResolveRawRef(ctx, repoID, rawRefCommit).Times(1).Return(&graveler.ResolvedRef{Type: graveler.ReferenceTypeCommit, BranchRecord: graveler.BranchRecord{Branch: &graveler.Branch{CommitID: commitID}}}, nil)
	}

	t.Run("get from branch - staging", func(t *testing.T) {
		test := initGravelerTest(t)
		setupGetFromBranch(test)

		test.stagingManager.EXPECT().Get(ctx, stagingToken1, key).Times(1).Return(value, nil)

		val, err := test.sut.Get(ctx, repoID, graveler.Ref(branchID), key)

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, value, val)
	})

	t.Run("get from branch - commit", func(t *testing.T) {
		test := initGravelerTest(t)
		setupGetFromBranch(test)

		test.stagingManager.EXPECT().Get(ctx, stagingToken1, key).Times(1).Return(nil, graveler.ErrNotFound)
		test.stagingManager.EXPECT().Get(ctx, stagingToken2, key).Times(1).Return(nil, graveler.ErrNotFound)
		test.stagingManager.EXPECT().Get(ctx, stagingToken3, key).Times(1).Return(nil, graveler.ErrNotFound)

		test.refManager.EXPECT().GetCommit(ctx, repoID, commitID).Times(1).Return(&commit, nil)
		test.committedManager.EXPECT().Get(ctx, repo.StorageNamespace, commit.MetaRangeID, key).Times(1).Return(value, nil)

		val, err := test.sut.Get(ctx, repoID, graveler.Ref(branchID), key)

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, value, val)
	})

	t.Run("get from branch - staging tombstone", func(t *testing.T) {
		test := initGravelerTest(t)
		setupGetFromBranch(test)

		test.stagingManager.EXPECT().Get(ctx, stagingToken1, key).Times(1).Return(nil, graveler.ErrNotFound)
		test.stagingManager.EXPECT().Get(ctx, stagingToken2, key).Times(1).Return(nil, nil)

		val, err := test.sut.Get(ctx, repoID, graveler.Ref(branchID), key)

		require.Error(t, graveler.ErrNotFound, err)
		require.Nil(t, val)
	})

	t.Run("get from branch - not found", func(t *testing.T) {
		test := initGravelerTest(t)
		setupGetFromBranch(test)

		test.stagingManager.EXPECT().Get(ctx, stagingToken1, key).Times(1).Return(nil, graveler.ErrNotFound)
		test.stagingManager.EXPECT().Get(ctx, stagingToken2, key).Times(1).Return(nil, graveler.ErrNotFound)
		test.stagingManager.EXPECT().Get(ctx, stagingToken3, key).Times(1).Return(nil, graveler.ErrNotFound)

		test.refManager.EXPECT().GetCommit(ctx, repoID, commitID).Times(1).Return(&commit, nil)
		test.committedManager.EXPECT().Get(ctx, repo.StorageNamespace, commit.MetaRangeID, key).Times(1).Return(nil, graveler.ErrNotFound)

		val, err := test.sut.Get(ctx, repoID, graveler.Ref(branchID), key)

		require.Error(t, graveler.ErrNotFound, err)
		require.Nil(t, val)
	})

	t.Run("get from commit", func(t *testing.T) {
		test := initGravelerTest(t)
		setupGetFromCommit(test)

		test.refManager.EXPECT().GetCommit(ctx, repoID, commitID).Times(1).Return(&commit, nil)
		test.committedManager.EXPECT().Get(ctx, repo.StorageNamespace, commit.MetaRangeID, key).Times(1).Return(value, nil)

		val, err := test.sut.Get(ctx, repoID, graveler.Ref(commitID), key)

		require.NoError(t, err)
		require.NotNil(t, val)
		require.Equal(t, value, val)
	})

	t.Run("get from commit - not found", func(t *testing.T) {
		test := initGravelerTest(t)
		setupGetFromCommit(test)

		test.refManager.EXPECT().GetCommit(ctx, repoID, commitID).Times(1).Return(&commit, nil)
		test.committedManager.EXPECT().Get(ctx, repo.StorageNamespace, commit.MetaRangeID, key).Times(1).Return(nil, graveler.ErrNotFound)

		val, err := test.sut.Get(ctx, repoID, graveler.Ref(commitID), key)

		require.Error(t, graveler.ErrNotFound, err)
		require.Nil(t, val)
	})

}
