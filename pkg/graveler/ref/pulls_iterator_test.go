package ref_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/kv/mock"
)

func TestPullsIterator(t *testing.T) {
	r, kvStore := testRefManager(t)
	ctx := context.Background()
	totalCount := 100
	now := time.Now().UTC()

	repository, err := r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageNamespace: "s3://foo",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	require.NoError(t, err)

	// prepare data
	for i := range totalCount {
		pid := fmt.Sprintf("pulls_%02d", i)
		require.NoError(t, r.CreatePullRequest(ctx, repository, graveler.PullRequestID(pid), &graveler.PullRequest{
			CreationDate:   now,
			Status:         graveler.PullRequestStatus(i % 2),
			Title:          pid,
			Author:         pid,
			Description:    pid,
			Source:         pid,
			Destination:    pid,
			MergedCommitID: &pid,
			ClosedDate:     &now,
		}))
	}

	t.Run("listing all pulls", func(t *testing.T) {
		iter, err := ref.NewPullsIterator(ctx, kvStore, repository)
		require.NoError(t, err)
		count := 0
		for iter.Next() {
			p := iter.Value()
			pid := fmt.Sprintf("pulls_%02d", count)
			require.Equal(t, &graveler.PullRequestRecord{
				ID: graveler.PullRequestID(pid),
				PullRequest: graveler.PullRequest{
					CreationDate:   now,
					Status:         graveler.PullRequestStatus(count % 2),
					Title:          pid,
					Author:         pid,
					Description:    pid,
					Source:         pid,
					Destination:    pid,
					MergedCommitID: &pid,
					ClosedDate:     &now,
				},
			}, p)
			count++
		}
		require.NoError(t, iter.Err())
		require.Equal(t, totalCount, count)
		iter.Close()
	})

	start := 30
	t.Run("listing pulls using prefix", func(t *testing.T) {
		iter, err := ref.NewPullsIterator(ctx, kvStore, repository)
		require.NoError(t, err)
		pid := fmt.Sprintf("pulls_%02d", start)
		iter.SeekGE(graveler.PullRequestID(pid))
		count := 0
		for iter.Next() {
			pid = fmt.Sprintf("pulls_%02d", start+count)
			p := iter.Value()
			require.Equal(t, pid, p.ID.String())
			count++
		}
		require.NoError(t, iter.Err())
		require.Equal(t, totalCount-start, count)
		iter.Close()
	})

	t.Run("listing pulls SeekGE", func(t *testing.T) {
		iter, err := ref.NewPullsIterator(ctx, kvStore, repository)
		require.NoError(t, err)

		pid := fmt.Sprintf("pulls_%02d", start)
		iter.SeekGE(graveler.PullRequestID(pid))

		for iter.Next() {
			// iterate till end
			continue
		}
		require.NoError(t, iter.Err())

		// now let's seek
		start = 20
		count := 0
		iter.SeekGE(graveler.PullRequestID(fmt.Sprintf("pulls_%02d", start)))
		for iter.Next() {
			pid = fmt.Sprintf("pulls_%02d", start+count)
			p := iter.Value()
			require.Equal(t, pid, p.ID.String())
			count++
		}
		require.NoError(t, iter.Err())
		require.Equal(t, totalCount-start, count)
		iter.Close()
	})

	t.Run("empty value SeekGE", func(t *testing.T) {
		iter, err := ref.NewPullsIterator(ctx, kvStore, repository)
		require.NoError(t, err)
		defer iter.Close()

		// make sure value is not nil
		require.True(t, iter.Next())

		// SeekGE should nil the value field
		pid := fmt.Sprintf("pulls_%02d", 5)
		iter.SeekGE(graveler.PullRequestID(pid))
		if iter.Value() != nil {
			t.Fatalf("expected nil value after seekGE")
		}
	})
}

func TestPullsIterator_CloseTwice(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	entIt := mock.NewMockEntriesIterator(ctrl)
	entIt.EXPECT().Close().Times(1)
	store := mock.NewMockStore(ctrl)
	store.EXPECT().Scan(ctx, gomock.Any(), gomock.Any()).Return(entIt, nil).Times(1)
	repo := &graveler.RepositoryRecord{
		RepositoryID: "repo",
		Repository: &graveler.Repository{
			InstanceUID: "rid",
		},
	}
	it, err := ref.NewPullsIterator(ctx, store, repo)
	require.NoError(t, err)
	it.Close()
	// Make sure calling Close again do not crash
	it.Close()
}

func TestPullsIterator_NextClosed(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	entIt := mock.NewMockEntriesIterator(ctrl)
	entIt.EXPECT().Close().Times(1)
	store := mock.NewMockStore(ctrl)
	store.EXPECT().Scan(ctx, gomock.Any(), gomock.Any()).Return(entIt, nil).Times(1)
	repo := &graveler.RepositoryRecord{
		RepositoryID: "repo",
		Repository: &graveler.Repository{
			InstanceUID: "rid",
		},
	}
	it, err := ref.NewPullsIterator(ctx, store, repo)
	require.NoError(t, err)
	it.Close()
	// Make sure calling Next should not crash
	it.Next()
}
