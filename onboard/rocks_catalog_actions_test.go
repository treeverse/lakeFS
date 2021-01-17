package onboard_test

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/onboard"
	"github.com/treeverse/lakefs/onboard/mock"
)

const (
	repoID         = graveler.RepositoryID("some-repo-id")
	metaRangeID    = graveler.MetaRangeID("some-mr-id")
	commitID       = graveler.CommitID("some-commit-id")
	parentCommitID = graveler.CommitID("some-parent-commit-id")
	committer      = "john-doe"
	msg            = "awesome-import-commit"
)

func TestFullCycleSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rangeManager := mock.NewMockmetaRangeManager(ctrl)

	mri := metaRangeID
	rangeManager.EXPECT().WriteMetaRange(gomock.Any(), gomock.Eq(repoID), gomock.Any()).Times(1).Return(&mri, nil)
	rangeManager.EXPECT().CommitExistingMetaRange(gomock.Any(), gomock.Eq(repoID), gomock.Eq(graveler.BranchID(catalog.DefaultImportBranchName)), gomock.Eq(mri), gomock.Eq(committer), gomock.Eq(msg), gomock.Any()).
		Times(1).Return(commitID, nil)

	rocks := onboard.NewRocksCatalogRepoActions(rangeManager, repoID, committer, logging.Default())

	validIt := getValidIt()
	stats, err := rocks.ApplyImport(context.Background(), validIt, parentCommitID, nil, false)
	require.NoError(t, err)
	require.NotNil(t, stats)

	retCommitID, err := rocks.Commit(context.Background(), msg, nil)
	require.NoError(t, err)
	require.Equal(t, string(commitID), retCommitID)
}

func TestApplyImportWrongIt(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rangeManager := mock.NewMockmetaRangeManager(ctrl)

	innerIt := getValidInnerIt()
	diffIt := onboard.NewDiffIterator(innerIt, innerIt)
	rocks := onboard.NewRocksCatalogRepoActions(rangeManager, repoID, committer, logging.Default())

	stats, err := rocks.ApplyImport(context.Background(), diffIt, parentCommitID, nil, false)
	require.Error(t, err)
	require.IsType(t, onboard.ErrWrongIterator, err)
	require.Nil(t, stats)
}

func TestApplyImportWriteFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rangeManager := mock.NewMockmetaRangeManager(ctrl)

	rangeManager.EXPECT().WriteMetaRange(gomock.Any(), gomock.Eq(repoID), gomock.Any()).Times(1).Return(nil, errors.New("some failure"))

	rocks := onboard.NewRocksCatalogRepoActions(rangeManager, repoID, committer, logging.Default())

	validIt := getValidIt()
	stats, err := rocks.ApplyImport(context.Background(), validIt, parentCommitID, nil, false)
	require.Error(t, err)
	require.Nil(t, stats)
}

func TestCommitBeforeApply(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rangeManager := mock.NewMockmetaRangeManager(ctrl)

	rocks := onboard.NewRocksCatalogRepoActions(rangeManager, repoID, committer, logging.Default())
	retCommitID, err := rocks.Commit(context.Background(), msg, nil)
	require.Error(t, err)
	require.Equal(t, "", retCommitID)
	require.Equal(t, onboard.ErrNoMetaRange, err)
}

func TestCommitFailed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	rangeManager := mock.NewMockmetaRangeManager(ctrl)

	mri := metaRangeID
	rangeManager.EXPECT().WriteMetaRange(gomock.Any(), gomock.Eq(repoID), gomock.Any()).Times(1).Return(&mri, nil)
	rangeManager.EXPECT().CommitExistingMetaRange(gomock.Any(), gomock.Eq(repoID), gomock.Eq(graveler.BranchID(catalog.DefaultImportBranchName)), gomock.Eq(mri), gomock.Eq(committer), gomock.Eq(msg), gomock.Any()).
		Times(1).Return(graveler.CommitID(""), errors.New("some-failure"))

	rocks := onboard.NewRocksCatalogRepoActions(rangeManager, repoID, committer, logging.Default())
	validIt := getValidIt()

	stats, err := rocks.ApplyImport(context.Background(), validIt, parentCommitID, nil, false)
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
