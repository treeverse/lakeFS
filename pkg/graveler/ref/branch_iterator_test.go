package ref_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestDBBranchIterator(t *testing.T) {
	r, db := testRefManagerWithDB(t)
	branches := []graveler.BranchID{"a", "aa", "b", "c", "e", "d"}
	ctx := context.Background()
	testutil.Must(t, r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageNamespace: "s3://foo",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	}, ""))

	// prepare data
	for _, b := range branches {
		testutil.Must(t, r.SetBranch(ctx, "repo1", b, graveler.Branch{CommitID: "c1"}))
	}

	t.Run("listing all branches", func(t *testing.T) {
		iter := ref.NewDBBranchIterator(ctx, db, "repo1", 3)
		ids := make([]graveler.BranchID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.BranchID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(ids, []graveler.BranchID{"a", "aa", "b", "c", "d", "e", "main"}); diffs != nil {
			t.Fatalf("got wrong list of IDs: %v", diffs)
		}
	})

	t.Run("listing branches SeekGE", func(t *testing.T) {
		iter := ref.NewDBBranchIterator(ctx, db, "repo1", 3)
		iter.SeekGE("b")
		ids := make([]graveler.BranchID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.BranchID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}

		if diffs := deep.Equal(ids, []graveler.BranchID{"b", "c", "d", "e", "main"}); diffs != nil {
			t.Fatalf("got wrong list of branch IDs: %v", diffs)
		}

		// now let's seek
		iter.SeekGE("aa")
		ids = make([]graveler.BranchID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.BranchID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(ids, []graveler.BranchID{"aa", "b", "c", "d", "e", "main"}); diffs != nil {
			t.Fatalf("got wrong list of branch IDs")
		}
	})
}

func TestBranchSimpleIterator(t *testing.T) {
	r, kvStore := testRefManagerWithKV(t)
	branches := []graveler.BranchID{"a", "aa", "b", "c", "e", "d"}
	ctx := context.Background()
	repo := &graveler.RepositoryRecord{
		RepositoryID: "repo1",
		Repository: &graveler.Repository{
			StorageNamespace: "s3://foo",
			CreationDate:     time.Now(),
			DefaultBranchID:  "main",
		},
	}
	testutil.Must(t, r.CreateRepository(ctx, repo.RepositoryID, *repo.Repository, ""))

	// prepare data
	for _, b := range branches {
		testutil.Must(t, r.SetBranch(ctx, "repo1", b, graveler.Branch{CommitID: "c1"}))
	}

	t.Run("listing all branches", func(t *testing.T) {
		iter, err := ref.NewBranchSimpleIterator(ctx, &kvStore, repo)
		require.NoError(t, err)
		ids := make([]graveler.BranchID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.BranchID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(ids, []graveler.BranchID{"a", "aa", "b", "c", "d", "e", "main"}); diffs != nil {
			t.Fatalf("got wrong list of IDs: %v", diffs)
		}
	})

	t.Run("listing branches SeekGE", func(t *testing.T) {
		iter, err := ref.NewBranchSimpleIterator(ctx, &kvStore, repo)
		require.NoError(t, err)
		iter.SeekGE("b")
		ids := make([]graveler.BranchID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.BranchID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}

		if diffs := deep.Equal(ids, []graveler.BranchID{"b", "c", "d", "e", "main"}); diffs != nil {
			t.Fatalf("got wrong list of branch IDs: %v", diffs)
		}

		// now let's seek
		iter.SeekGE("aa")
		ids = make([]graveler.BranchID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.BranchID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(ids, []graveler.BranchID{"aa", "b", "c", "d", "e", "main"}); diffs != nil {
			t.Fatalf("got wrong list of branch IDs")
		}

		require.False(t, iter.Next())
		require.Nil(t, iter.Value())
		require.ErrorIs(t, iter.Err(), ref.ErrIteratorClosed)
	})
}

func TestBranchByCommitIterator(t *testing.T) {
	r, kvStore := testRefManagerWithKV(t)
	// TODO niro: Need commits PR (remove 'main' from list when done)
	branches := []graveler.BranchID{"a", "aa", "b", "c", "e", "d", "main"}
	ctx := context.Background()
	repo := &graveler.RepositoryRecord{
		RepositoryID: "repo1",
		Repository: &graveler.Repository{
			StorageNamespace: "s3://foo",
			CreationDate:     time.Now(),
			DefaultBranchID:  "main",
		},
	}
	testutil.Must(t, r.CreateRepository(ctx, repo.RepositoryID, *repo.Repository, ""))

	// prepare data
	for i, b := range branches {
		testutil.Must(t, r.SetBranch(ctx, repo.RepositoryID, b, graveler.Branch{CommitID: graveler.CommitID(branches[len(branches)-i-1])}))
	}

	t.Run("listing all branches", func(t *testing.T) {
		iter, err := ref.NewBranchByCommitIterator(ctx, &kvStore, repo)
		require.NoError(t, err)
		ids := make([]graveler.CommitID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.CommitID)
		}
		fmt.Println(ids)
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(ids, []graveler.CommitID{"a", "aa", "b", "c", "d", "e", "main"}); diffs != nil {
			t.Fatalf("got wrong list of IDs: %v", diffs)
		}
	})
}
