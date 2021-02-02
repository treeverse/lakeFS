package onboard_test

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/catalog/testutils"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/onboard"
	"github.com/treeverse/lakefs/onboard/mock"
)

const (
	repoID      = graveler.RepositoryID("some-repo-id")
	metaRangeID = graveler.MetaRangeID("some-mr-id")
	commitID    = graveler.CommitID("some-commit-id")
	committer   = "john-doe"
	msg         = "awesome-import-commit"
)

func TestFullCyclePorcelainImportBranchExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rangeManager := mock.NewMockentryCataloger(ctrl)

	mri := metaRangeID
	prevCommitID := graveler.CommitID("somePrevCommitID")
	rangeManager.EXPECT().
		GetBranch(gomock.Any(), gomock.Eq(repoID), gomock.Eq(graveler.BranchID(catalog.DefaultImportBranchName))).
		Times(1).
		Return(&graveler.Branch{
			CommitID: prevCommitID,
		}, nil)

	rangeManager.EXPECT().
		ListEntries(gomock.Any(), gomock.Eq(repoID), gomock.Eq(graveler.Ref(catalog.DefaultImportBranchName)), gomock.Any(), gomock.Any()).
		Times(1).
		Return(catalog.NewEntryListingIterator(testutils.NewFakeEntryIterator([]*catalog.EntryRecord{
			{
				Path:  "some/path",
				Entry: &catalog.Entry{},
			},
		}), "", ""), nil)
	rangeManager.EXPECT().WriteMetaRange(gomock.Any(), gomock.Eq(repoID), gomock.Any()).Times(1).Return(&mri, nil)
	rangeManager.EXPECT().AddCommitToBranchHead(gomock.Any(), gomock.Eq(repoID), gomock.Eq(graveler.BranchID(catalog.DefaultImportBranchName)), gomock.Any()).
		Times(1).Return(commitID, nil)

	rocks := onboard.NewCatalogRepoActions(rangeManager, repoID, committer, logging.Default(), nil)

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
	rangeManager := mock.NewMockentryCataloger(ctrl)

	mri := metaRangeID
	prevCommitID := graveler.CommitID("somePrevCommitID")
	rangeManager.EXPECT().
		GetBranch(gomock.Any(), gomock.Eq(repoID), gomock.Eq(graveler.BranchID(catalog.DefaultImportBranchName))).
		Times(1).
		Return(nil, graveler.ErrBranchNotFound)

	rangeManager.EXPECT().
		CreateBranch(gomock.Any(), gomock.Eq(repoID), gomock.Eq(graveler.BranchID(catalog.DefaultImportBranchName)), gomock.Eq(graveler.Ref(catalog.DefaultBranchName))).
		Times(1).
		Return(&graveler.Branch{
			CommitID: prevCommitID,
		}, nil)

	rangeManager.EXPECT().
		ListEntries(gomock.Any(), gomock.Eq(repoID), gomock.Eq(graveler.Ref(catalog.DefaultImportBranchName)), gomock.Any(), gomock.Any()).
		Times(1).
		Return(catalog.NewEntryListingIterator(testutils.NewFakeEntryIterator([]*catalog.EntryRecord{
			{
				Path:  "some/path",
				Entry: &catalog.Entry{},
			},
		}), "", ""), nil)
	rangeManager.EXPECT().WriteMetaRange(gomock.Any(), gomock.Eq(repoID), gomock.Any()).Times(1).Return(&mri, nil)
	rangeManager.EXPECT().AddCommitToBranchHead(gomock.Any(), gomock.Eq(repoID), gomock.Eq(graveler.BranchID(catalog.DefaultImportBranchName)), gomock.Any()).
		Times(1).Return(commitID, nil)

	rocks := onboard.NewCatalogRepoActions(rangeManager, repoID, committer, logging.Default(), nil)

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
	rangeManager := mock.NewMockentryCataloger(ctrl)

	mri := metaRangeID
	prevCommitID := graveler.CommitID("somePrevCommitID")

	rangeManager.EXPECT().
		ListEntries(gomock.Any(), gomock.Eq(repoID), gomock.Eq(graveler.Ref(prevCommitID)), gomock.Any(), gomock.Any()).
		Times(1).
		Return(catalog.NewEntryListingIterator(testutils.NewFakeEntryIterator([]*catalog.EntryRecord{
			{
				Path:  "some/path",
				Entry: &catalog.Entry{},
			},
		}), "", ""), nil)
	rangeManager.EXPECT().WriteMetaRange(gomock.Any(), gomock.Eq(repoID), gomock.Any()).Times(1).Return(&mri, nil)
	rangeManager.EXPECT().AddCommit(gomock.Any(), gomock.Eq(repoID), gomock.Any()).
		Times(1).Return(commitID, nil)

	rocks := onboard.NewCatalogRepoActions(rangeManager, repoID, committer, logging.Default(), nil)

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
	rangeManager := mock.NewMockentryCataloger(ctrl)

	prevCommitID := graveler.CommitID("somePrevCommitID")
	rangeManager.EXPECT().
		GetBranch(gomock.Any(), gomock.Eq(repoID), gomock.Eq(graveler.BranchID(catalog.DefaultImportBranchName))).
		Times(1).
		Return(&graveler.Branch{
			CommitID: prevCommitID,
		}, nil)

	rangeManager.EXPECT().
		ListEntries(gomock.Any(), gomock.Eq(repoID), gomock.Eq(graveler.Ref(catalog.DefaultImportBranchName)), gomock.Any(), gomock.Any()).
		Times(1).
		Return(catalog.NewEntryListingIterator(testutils.NewFakeEntryIterator([]*catalog.EntryRecord{
			{
				Path:  "some/path",
				Entry: &catalog.Entry{},
			},
		}), "", ""), nil)
	rangeManager.EXPECT().WriteMetaRange(gomock.Any(), gomock.Eq(repoID), gomock.Any()).Times(1).Return(nil, errors.New("some failure"))

	rocks := onboard.NewCatalogRepoActions(rangeManager, repoID, committer, logging.Default(), nil)

	require.NoError(t, rocks.Init(context.Background(), ""))

	validIt := getValidIt()
	stats, err := rocks.ApplyImport(context.Background(), validIt, false)
	require.Error(t, err)
	require.Nil(t, stats)
}

func TestCommitBeforeApply(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rangeManager := mock.NewMockentryCataloger(ctrl)

	rocks := onboard.NewCatalogRepoActions(rangeManager, repoID, committer, logging.Default(), nil)
	retCommitID, err := rocks.Commit(context.Background(), msg, nil)
	require.Error(t, err)
	require.Equal(t, "", retCommitID)
	require.Equal(t, onboard.ErrNoMetaRange, err)
}

func TestCommitFailed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rangeManager := mock.NewMockentryCataloger(ctrl)

	mri := metaRangeID
	prevCommitID := graveler.CommitID("somePrevCommitID")
	rangeManager.EXPECT().
		GetBranch(gomock.Any(), gomock.Eq(repoID), gomock.Eq(graveler.BranchID(catalog.DefaultImportBranchName))).
		Times(1).
		Return(&graveler.Branch{
			CommitID: prevCommitID,
		}, nil)

	rangeManager.EXPECT().
		ListEntries(gomock.Any(), gomock.Eq(repoID), gomock.Eq(graveler.Ref(catalog.DefaultImportBranchName)), gomock.Any(), gomock.Any()).
		Times(1).
		Return(catalog.NewEntryListingIterator(testutils.NewFakeEntryIterator([]*catalog.EntryRecord{
			{
				Path:  "some/path",
				Entry: &catalog.Entry{},
			},
		}), "", ""), nil)
	rangeManager.EXPECT().WriteMetaRange(gomock.Any(), gomock.Eq(repoID), gomock.Any()).Times(1).Return(&mri, nil)

	rangeManager.EXPECT().AddCommitToBranchHead(gomock.Any(), gomock.Eq(repoID), gomock.Eq(graveler.BranchID(catalog.DefaultImportBranchName)), gomock.Any()).
		Times(1).Return(graveler.CommitID(""), errors.New("some-failure"))

	rocks := onboard.NewCatalogRepoActions(rangeManager, repoID, committer, logging.Default(), nil)
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
