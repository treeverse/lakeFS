package ref_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestDBRepositoryIterator(t *testing.T) {
	r, db := testRefManagerWithDB(t)
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
		iter := ref.NewDBRepositoryIterator(context.Background(), db, 3)
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
		iter := ref.NewDBRepositoryIterator(context.Background(), db, 3)
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
		iter := ref.NewDBRepositoryIterator(context.Background(), db, 3)
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

func TestKVRepositoryIterator(t *testing.T) {
	r, store := testRefManagerWithKV(t)
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
		iter, err := ref.NewKVRepositoryIterator(context.Background(), &store)
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
		iter, err := ref.NewKVRepositoryIterator(context.Background(), &store)
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
		iter, err := ref.NewKVRepositoryIterator(context.Background(), &store)
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
