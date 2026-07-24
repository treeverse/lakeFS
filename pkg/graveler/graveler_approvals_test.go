package graveler_test

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/testutil"
)

func TestGraveler_AddPullRequestApproval(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pullID := graveler.PullRequestID(xid.New().String())
	basePR := graveler.PullRequest{
		Status: graveler.PullRequestStatus_OPEN,
		Author: "author",
	}

	expectUpdate := func(test *testutil.GravelerTest, pr graveler.PullRequest, check func(t *testing.T, newPR *graveler.PullRequest, err error)) {
		test.RefManager.EXPECT().UpdatePullRequest(ctx, repository, pullID, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.PullRequestID, f graveler.PullUpdateFunc) error {
				newPR, err := f(&pr)
				check(t, newPR, err)
				return err
			}).Times(1)
	}

	t.Run("first approval", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		expectUpdate(test, basePR, func(t *testing.T, newPR *graveler.PullRequest, err error) {
			require.NoError(t, err)
			require.Len(t, newPR.Approvals, 1)
			require.Equal(t, "reviewer1", newPR.Approvals[0].Approver)
			require.Equal(t, "commit1", newPR.Approvals[0].SourceCommitID)
		})
		err := test.Sut.AddPullRequestApproval(ctx, repository, pullID, "reviewer1", "commit1")
		require.NoError(t, err)
	})

	t.Run("re-approval refreshes source commit instead of duplicating", func(t *testing.T) {
		pr := basePR
		pr.Approvals = []graveler.PullRequestApproval{
			{Approver: "reviewer1", CreationDate: time.Now(), SourceCommitID: "commit1"},
		}
		test := testutil.InitGravelerTest(t)
		expectUpdate(test, pr, func(t *testing.T, newPR *graveler.PullRequest, err error) {
			require.NoError(t, err)
			require.Len(t, newPR.Approvals, 1)
			require.Equal(t, "commit2", newPR.Approvals[0].SourceCommitID)
		})
		err := test.Sut.AddPullRequestApproval(ctx, repository, pullID, "reviewer1", "commit2")
		require.NoError(t, err)
	})

	t.Run("author cannot approve own pull request", func(t *testing.T) {
		test := testutil.InitGravelerTest(t)
		expectUpdate(test, basePR, func(t *testing.T, newPR *graveler.PullRequest, err error) {
			require.ErrorIs(t, err, graveler.ErrPullRequestApprovalByAuthor)
			require.Nil(t, newPR)
		})
		err := test.Sut.AddPullRequestApproval(ctx, repository, pullID, "author", "commit1")
		require.ErrorIs(t, err, graveler.ErrPullRequestApprovalByAuthor)
	})

	t.Run("cannot approve a closed pull request", func(t *testing.T) {
		pr := basePR
		pr.Status = graveler.PullRequestStatus_CLOSED
		test := testutil.InitGravelerTest(t)
		expectUpdate(test, pr, func(t *testing.T, newPR *graveler.PullRequest, err error) {
			require.ErrorIs(t, err, graveler.ErrPullRequestNotOpen)
			require.Nil(t, newPR)
		})
		err := test.Sut.AddPullRequestApproval(ctx, repository, pullID, "reviewer1", "commit1")
		require.ErrorIs(t, err, graveler.ErrPullRequestNotOpen)
	})
}

func TestGraveler_RemovePullRequestApproval(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pullID := graveler.PullRequestID(xid.New().String())

	t.Run("removes matching approval", func(t *testing.T) {
		pr := graveler.PullRequest{
			Status: graveler.PullRequestStatus_OPEN,
			Author: "author",
			Approvals: []graveler.PullRequestApproval{
				{Approver: "reviewer1", CreationDate: time.Now(), SourceCommitID: "commit1"},
				{Approver: "reviewer2", CreationDate: time.Now(), SourceCommitID: "commit1"},
			},
		}
		test := testutil.InitGravelerTest(t)
		test.RefManager.EXPECT().UpdatePullRequest(ctx, repository, pullID, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.PullRequestID, f graveler.PullUpdateFunc) error {
				newPR, err := f(&pr)
				require.NoError(t, err)
				require.Len(t, newPR.Approvals, 1)
				require.Equal(t, "reviewer2", newPR.Approvals[0].Approver)
				return nil
			}).Times(1)
		err := test.Sut.RemovePullRequestApproval(ctx, repository, pullID, "reviewer1")
		require.NoError(t, err)
	})

	t.Run("no-op when approver never approved", func(t *testing.T) {
		pr := graveler.PullRequest{Status: graveler.PullRequestStatus_OPEN, Author: "author"}
		test := testutil.InitGravelerTest(t)
		test.RefManager.EXPECT().UpdatePullRequest(ctx, repository, pullID, gomock.Any()).
			DoAndReturn(func(_ context.Context, _ *graveler.RepositoryRecord, _ graveler.PullRequestID, f graveler.PullUpdateFunc) error {
				newPR, err := f(&pr)
				require.NoError(t, err)
				require.Nil(t, newPR)
				return nil
			}).Times(1)
		err := test.Sut.RemovePullRequestApproval(ctx, repository, pullID, "reviewer1")
		require.NoError(t, err)
	})
}
