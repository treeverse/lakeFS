package onboard_test

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog/testutils"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/onboard"
	"github.com/treeverse/lakefs/pkg/onboard/mock"
)

const (
	repoID      = graveler.RepositoryID("some-repo-id")
	metaRangeID = graveler.MetaRangeID("some-mr-id")
	commitID    = graveler.CommitID("some-commit-id")
	committer   = "john-doe"
	msg         = "awesome-import-commit"
)

var repository = &graveler.RepositoryRecord{RepositoryID: repoID}

func TestFullCyclePorcelainImportBranchExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rangeManager := mock.NewMockEntryCatalog(ctrl)

	rangeManager.EXPECT().
		GetRepository(gomock.Any(), gomock.Eq(repoID)).
		Times(1).
		Return(repository, nil)

	mri := metaRangeID
	prevCommitID := graveler.CommitID("somePrevCommitID")
	rangeManager.EXPECT().
		GetBranch(gomock.Any(), gomock.Eq(repository), gomock.Eq(graveler.BranchID(onboard.DefaultImportBranchName))).
		Times(1).
		Return(&graveler.Branch{
			CommitID: prevCommitID,
		}, nil)

	rangeManager.EXPECT().
		List(gomock.Any(), gomock.Eq(repository), gomock.Eq(graveler.Ref(onboard.DefaultImportBranchName))).
		Times(1).
		Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{
			{
				Key:   graveler.Key("some/path"),
				Value: nil,
			},
		}), nil)
	rangeManager.EXPECT().WriteMetaRangeByIterator(gomock.Any(), gomock.Eq(repository), gomock.Any()).Times(1).Return(&mri, nil)
	rangeManager.EXPECT().GetCommit(gomock.Any(), gomock.Eq(repository), gomock.Eq(prevCommitID)).Return(&graveler.Commit{}, nil)
	rangeManager.EXPECT().AddCommitToBranchHead(gomock.Any(), gomock.Eq(repository), gomock.Eq(graveler.BranchID(onboard.DefaultImportBranchName)), gomock.Any()).
		Times(1).Return(commitID, nil)

	rocks := onboard.NewCatalogRepoActions(&onboard.Config{
		CommitUsername:  committer,
		RepositoryID:    repoID,
		DefaultBranchID: "main",
		Store:           rangeManager,
	}, logging.Default())

	require.NoError(t, rocks.Init(context.Background(), ""))

	validIt := getValidIt()
	stats, err := rocks.ApplyImport(context.Background(), validIt, false)
	require.NoError(t, err)
	require.NotNil(t, stats)

	retCommitID, err := rocks.Commit(context.Background(), msg, nil)
	require.NoError(t, err)
	require.Equal(t, string(commitID), retCommitID)
}

func TestFullCyclePorcelainImportBranchMissing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rangeManager := mock.NewMockEntryCatalog(ctrl)

	rangeManager.EXPECT().
		GetRepository(gomock.Any(), gomock.Eq(repoID)).
		Times(1).
		Return(repository, nil)

	mri := metaRangeID
	prevCommitID := graveler.CommitID("somePrevCommitID")
	rangeManager.EXPECT().
		GetBranch(gomock.Any(), gomock.Eq(repository), gomock.Eq(graveler.BranchID(onboard.DefaultImportBranchName))).
		Times(1).
		Return(nil, graveler.ErrBranchNotFound)

	rangeManager.EXPECT().
		CreateBranch(gomock.Any(), gomock.Eq(repository), gomock.Eq(graveler.BranchID(onboard.DefaultImportBranchName)), gomock.Eq(graveler.Ref("main"))).
		Times(1).
		Return(&graveler.Branch{
			CommitID: prevCommitID,
		}, nil)

	rangeManager.EXPECT().
		List(gomock.Any(), gomock.Eq(repository), gomock.Eq(graveler.Ref(onboard.DefaultImportBranchName))).
		Times(1).
		Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{
			{
				Key:   graveler.Key("some/path"),
				Value: nil,
			},
		}), nil)
	rangeManager.EXPECT().WriteMetaRangeByIterator(gomock.Any(), gomock.Eq(repository), gomock.Any()).Times(1).Return(&mri, nil)
	rangeManager.EXPECT().GetCommit(gomock.Any(), gomock.Eq(repository), gomock.Eq(prevCommitID)).Return(&graveler.Commit{}, nil)
	rangeManager.EXPECT().AddCommitToBranchHead(gomock.Any(), gomock.Eq(repository), gomock.Eq(graveler.BranchID(onboard.DefaultImportBranchName)), gomock.Any()).
		Times(1).Return(commitID, nil)

	rocks := onboard.NewCatalogRepoActions(&onboard.Config{
		CommitUsername:  committer,
		RepositoryID:    repoID,
		DefaultBranchID: "main",
		Store:           rangeManager,
	}, logging.Default())

	require.NoError(t, rocks.Init(context.Background(), ""))

	validIt := getValidIt()
	stats, err := rocks.ApplyImport(context.Background(), validIt, false)
	require.NoError(t, err)
	require.NotNil(t, stats)

	retCommitID, err := rocks.Commit(context.Background(), msg, nil)
	require.NoError(t, err)
	require.Equal(t, string(commitID), retCommitID)
}

func TestFullCyclePlumbing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rangeManager := mock.NewMockEntryCatalog(ctrl)

	rangeManager.EXPECT().
		GetRepository(gomock.Any(), gomock.Eq(repoID)).
		Times(1).
		Return(repository, nil)

	mri := metaRangeID
	prevCommitID := graveler.CommitID("somePrevCommitID")

	rangeManager.EXPECT().
		List(gomock.Any(), gomock.Eq(repository), gomock.Eq(graveler.Ref(prevCommitID))).
		Times(1).
		Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{
			{
				Key:   graveler.Key("some/path"),
				Value: nil,
			},
		}), nil)
	rangeManager.EXPECT().WriteMetaRangeByIterator(gomock.Any(), gomock.Eq(repository), gomock.Any()).Times(1).Return(&mri, nil)
	rangeManager.EXPECT().GetCommit(gomock.Any(), gomock.Eq(repository), gomock.Eq(prevCommitID)).Return(&graveler.Commit{}, nil)
	rangeManager.EXPECT().AddCommit(gomock.Any(), gomock.Eq(repository), gomock.Any()).
		Times(1).Return(commitID, nil)

	rocks := onboard.NewCatalogRepoActions(&onboard.Config{
		CommitUsername:  committer,
		RepositoryID:    repoID,
		DefaultBranchID: "main",
		Store:           rangeManager,
	}, logging.Default())

	require.NoError(t, rocks.Init(context.Background(), prevCommitID))

	validIt := getValidIt()
	stats, err := rocks.ApplyImport(context.Background(), validIt, false)
	require.NoError(t, err)
	require.NotNil(t, stats)

	retCommitID, err := rocks.Commit(context.Background(), msg, nil)
	require.NoError(t, err)
	require.Equal(t, string(commitID), retCommitID)
}

func TestApplyImportWriteFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rangeManager := mock.NewMockEntryCatalog(ctrl)

	rangeManager.EXPECT().
		GetRepository(gomock.Any(), gomock.Eq(repoID)).
		Times(1).
		Return(repository, nil)

	prevCommitID := graveler.CommitID("somePrevCommitID")
	rangeManager.EXPECT().
		GetBranch(gomock.Any(), gomock.Eq(repository), gomock.Eq(graveler.BranchID(onboard.DefaultImportBranchName))).
		Times(1).
		Return(&graveler.Branch{
			CommitID: prevCommitID,
		}, nil)

	rangeManager.EXPECT().
		List(gomock.Any(), gomock.Eq(repository), gomock.Eq(graveler.Ref(onboard.DefaultImportBranchName))).
		Times(1).
		Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{
			{
				Key:   graveler.Key("some/path"),
				Value: nil,
			},
		}), nil)
	rangeManager.EXPECT().WriteMetaRangeByIterator(gomock.Any(), gomock.Eq(repository), gomock.Any()).Times(1).Return(nil, errors.New("some failure"))

	rocks := onboard.NewCatalogRepoActions(&onboard.Config{
		CommitUsername:  committer,
		RepositoryID:    repoID,
		DefaultBranchID: "main",
		Store:           rangeManager,
	}, logging.Default())

	require.NoError(t, rocks.Init(context.Background(), ""))

	validIt := getValidIt()
	stats, err := rocks.ApplyImport(context.Background(), validIt, false)
	require.Error(t, err)
	require.Nil(t, stats)
}

func TestCommitBeforeApply(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rangeManager := mock.NewMockEntryCatalog(ctrl)

	rocks := onboard.NewCatalogRepoActions(&onboard.Config{
		CommitUsername:  committer,
		RepositoryID:    repoID,
		DefaultBranchID: "main",
		Store:           rangeManager,
	}, logging.Default())
	retCommitID, err := rocks.Commit(context.Background(), msg, nil)
	require.Error(t, err)
	require.Equal(t, "", retCommitID)
	require.Equal(t, onboard.ErrNoMetaRange, err)
}

func TestCommitFailed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rangeManager := mock.NewMockEntryCatalog(ctrl)

	rangeManager.EXPECT().
		GetRepository(gomock.Any(), gomock.Eq(repoID)).
		Times(1).
		Return(repository, nil)

	mri := metaRangeID
	prevCommitID := graveler.CommitID("somePrevCommitID")
	rangeManager.EXPECT().
		GetBranch(gomock.Any(), gomock.Eq(repository), gomock.Eq(graveler.BranchID(onboard.DefaultImportBranchName))).
		Times(1).
		Return(&graveler.Branch{
			CommitID: prevCommitID,
		}, nil)

	rangeManager.EXPECT().
		List(gomock.Any(), gomock.Eq(repository), gomock.Eq(graveler.Ref(onboard.DefaultImportBranchName))).
		Times(1).
		Return(testutils.NewFakeValueIterator([]*graveler.ValueRecord{
			{
				Key:   graveler.Key("some/path"),
				Value: nil,
			},
		}), nil)
	rangeManager.EXPECT().WriteMetaRangeByIterator(gomock.Any(), gomock.Eq(repository), gomock.Any()).Times(1).Return(&mri, nil)
	rangeManager.EXPECT().GetCommit(gomock.Any(), gomock.Eq(repository), gomock.Eq(prevCommitID)).Return(&graveler.Commit{}, nil)
	rangeManager.EXPECT().AddCommitToBranchHead(gomock.Any(), gomock.Eq(repository), gomock.Eq(graveler.BranchID(onboard.DefaultImportBranchName)), gomock.Any()).
		Times(1).Return(graveler.CommitID(""), errors.New("some-failure"))

	rocks := onboard.NewCatalogRepoActions(&onboard.Config{
		CommitUsername:  committer,
		RepositoryID:    repoID,
		DefaultBranchID: "main",
		Store:           rangeManager,
	}, logging.Default())
	require.NoError(t, rocks.Init(context.Background(), ""))

	validIt := getValidIt()

	stats, err := rocks.ApplyImport(context.Background(), validIt, false)
	require.NoError(t, err)
	require.NotNil(t, stats)

	retCommitID, err := rocks.Commit(context.Background(), msg, nil)
	require.Error(t, err)
	require.Equal(t, "", retCommitID)
}

func getValidIt() *onboard.InventoryIterator {
	return onboard.NewInventoryIterator(getValidInnerIt())
}

func getValidInnerIt() block.InventoryIterator {
	return &mockInventoryIterator{
		rows: []block.InventoryObject{
			{
				Bucket:          "bucket-1",
				Key:             "key-1",
				Size:            1024,
				Checksum:        "checksum-1",
				PhysicalAddress: "/some/path/to/object",
			},
		}}
}
