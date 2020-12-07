package graveler_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-test/deep"

	"github.com/treeverse/lakefs/graveler"

	"github.com/treeverse/lakefs/testutil"
)

func TestPGRepositoryIterator(t *testing.T) {
	r, db := testRefManagerWithDB(t)
	repos := []graveler.RepositoryID{"a", "aa", "b", "c", "e", "d"}

	// prepare data
	for _, repoId := range repos {
		testutil.Must(t, r.CreateRepository(context.Background(), repoId, graveler.Repository{
			StorageNamespace: "s3://foo",
			CreationDate:     time.Now(),
			DefaultBranchID:  "master",
		}, graveler.Branch{}))
	}

	t.Run("listing all repos", func(t *testing.T) {
		iter := graveler.NewRepositoryIterator(context.Background(), db, 3, "")
		repoIds := make([]graveler.RepositoryID, 0)
		for iter.Next() {
			repo := iter.Value()
			repoIds = append(repoIds, repo.RepositoryID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(repoIds, []graveler.RepositoryID{"a", "aa", "b", "c", "d", "e"}); diffs != nil {
			t.Fatalf("got wrong list of repo IDs: %v", diffs)
		}
	})

	t.Run("listing repos from prefix", func(t *testing.T) {
		iter := graveler.NewRepositoryIterator(context.Background(), db, 3, "b")
		repoIds := make([]graveler.RepositoryID, 0)
		for iter.Next() {
			repo := iter.Value()
			repoIds = append(repoIds, repo.RepositoryID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(repoIds, []graveler.RepositoryID{"b", "c", "d", "e"}); diffs != nil {
			t.Fatalf("got wrong list of repo IDs: %v", diffs)
		}
	})

	t.Run("listing repos SeekGE", func(t *testing.T) {
		iter := graveler.NewRepositoryIterator(context.Background(), db, 3, "b")
		repoIds := make([]graveler.RepositoryID, 0)
		for iter.Next() {
			repo := iter.Value()
			repoIds = append(repoIds, repo.RepositoryID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(repoIds, []graveler.RepositoryID{"b", "c", "d", "e"}); diffs != nil {
			t.Fatalf("got wrong list of repo IDs: %v", diffs)
		}

		// now let's seek
		if !iter.SeekGE("aa") {
			t.Fatalf("we should have values here")
		}

		repoIds = make([]graveler.RepositoryID, 0)
		for iter.Next() {
			repo := iter.Value()
			repoIds = append(repoIds, repo.RepositoryID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(repoIds, []graveler.RepositoryID{"aa", "b", "c", "d", "e"}); diffs != nil {
			t.Fatalf("got wrong list of repo IDs: %v", diffs)
		}
	})
}

func TestPGBranchIterator(t *testing.T) {
	r, db := testRefManagerWithDB(t)
	branches := []graveler.BranchID{"a", "aa", "b", "c", "e", "d"}
	testutil.Must(t, r.CreateRepository(context.Background(), "repo1", graveler.Repository{
		StorageNamespace: "s3://foo",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, graveler.Branch{}))

	// prepare data
	for _, b := range branches {
		testutil.Must(t, r.SetBranch(context.Background(), "repo1", b, graveler.Branch{CommitID: "c1"}))
	}

	t.Run("listing all branches", func(t *testing.T) {
		iter := graveler.NewBranchIterator(context.Background(), db, "repo1", 3, "")
		ids := make([]graveler.BranchID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.BranchID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(ids, []graveler.BranchID{"a", "aa", "b", "c", "d", "e", "master"}); diffs != nil {
			t.Fatalf("got wrong list of IDs: %v", diffs)
		}
	})

	t.Run("listing branches from prefix", func(t *testing.T) {
		iter := graveler.NewBranchIterator(context.Background(), db, "repo1", 3, "b")
		ids := make([]graveler.BranchID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.BranchID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(ids, []graveler.BranchID{"b", "c", "d", "e", "master"}); diffs != nil {
			t.Fatalf("got wrong list of branch IDs: %v", diffs)
		}
	})

	t.Run("listing branches SeekGE", func(t *testing.T) {
		iter := graveler.NewBranchIterator(context.Background(), db, "repo1", 3, "b")
		ids := make([]graveler.BranchID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.BranchID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(ids, []graveler.BranchID{"b", "c", "d", "e", "master"}); diffs != nil {
			t.Fatalf("got wrong list of branch IDs: %v", diffs)
		}

		// now let's seek
		if !iter.SeekGE("aa") {
			t.Fatalf("we should have values here")
		}

		ids = make([]graveler.BranchID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.BranchID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(ids, []graveler.BranchID{"aa", "b", "c", "d", "e", "master"}); diffs != nil {
			t.Fatalf("got wrong list of branch IDs")
		}
	})
}
