package ref_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/kv/mock"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestRepositoryIterator(t *testing.T) {
	r, store := testRefManager(t)
	repos := []graveler.RepositoryID{"a", "aa", "b", "c", "e", "d", "f"}

	// prepare data
	for _, repoId := range repos {
		_, err := r.CreateRepository(context.Background(), repoId, graveler.Repository{
			StorageNamespace: "s3://foo",
			CreationDate:     time.Now(),
			DefaultBranchID:  "main",
		})
		testutil.Must(t, err)
	}

	t.Run("listing all repos", func(t *testing.T) {
		iter, err := ref.NewRepositoryIterator(context.Background(), store)
		require.NoError(t, err)
		repoIds := make([]graveler.RepositoryID, 0)
		for iter.Next() {
			repo := iter.Value()
			repoIds = append(repoIds, repo.RepositoryID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(repoIds, []graveler.RepositoryID{"a", "aa", "b", "c", "d", "e", "f"}); diffs != nil {
			t.Fatalf("got wrong list of repo IDs: %v", diffs)
		}
	})

	t.Run("listing repos from prefix", func(t *testing.T) {
		iter, err := ref.NewRepositoryIterator(context.Background(), store)
		require.NoError(t, err)
		iter.SeekGE("b")
		repoIds := make([]graveler.RepositoryID, 0)
		for iter.Next() {
			repo := iter.Value()
			repoIds = append(repoIds, repo.RepositoryID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(repoIds, []graveler.RepositoryID{"b", "c", "d", "e", "f"}); diffs != nil {
			t.Fatalf("got wrong list of repo IDs: %v", diffs)
		}
	})

	t.Run("listing repos SeekGE", func(t *testing.T) {
		iter, err := ref.NewRepositoryIterator(context.Background(), store)
		require.NoError(t, err)
		iter.SeekGE("b")
		repoIds := make([]graveler.RepositoryID, 0)
		for iter.Next() {
			repo := iter.Value()
			repoIds = append(repoIds, repo.RepositoryID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}

		if diffs := deep.Equal(repoIds, []graveler.RepositoryID{"b", "c", "d", "e", "f"}); diffs != nil {
			t.Fatalf("got wrong list of repo IDs: %v", diffs)
		}

		// now let's seek
		iter.SeekGE("aa")

		repoIds = make([]graveler.RepositoryID, 0)
		for iter.Next() {
			repo := iter.Value()
			repoIds = append(repoIds, repo.RepositoryID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(repoIds, []graveler.RepositoryID{"aa", "b", "c", "d", "e", "f"}); diffs != nil {
			t.Fatalf("got wrong list of repo IDs: %v", diffs)
		}
	})
}

func TestRepositoryIterator_CloseTwice(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	entIt := mock.NewMockEntriesIterator(ctrl)
	entIt.EXPECT().Close().Times(1)
	store := mock.NewMockStore(ctrl)
	store.EXPECT().Scan(ctx, gomock.Any(), gomock.Any()).Return(entIt, nil).Times(1)
	it, err := ref.NewRepositoryIterator(ctx, store)
	if err != nil {
		t.Fatal("NewRepositoryIterator failed", err)
	}
	it.Close()
	// Make sure calling Close again do not crash
	it.Close()
}

func TestRepositoryIterator_NextClosed(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	entIt := mock.NewMockEntriesIterator(ctrl)
	entIt.EXPECT().Close().Times(1)
	store := mock.NewMockStore(ctrl)
	store.EXPECT().Scan(ctx, gomock.Any(), gomock.Any()).Return(entIt, nil).Times(1)
	it, err := ref.NewRepositoryIterator(ctx, store)
	if err != nil {
		t.Fatal("NewRepositoryIterator failed", err)
	}
	it.Close()
	// Make sure calling Next should not crash
	it.Next()
}
