package rocks_test

import (
	"context"
	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/testutil"
	"reflect"
	"testing"
	"time"
)

func TestPGRepositoryIterator(t *testing.T) {
	r, db := testRefManagerWithDB(t)
	repos := []rocks.RepositoryID{"a", "aa", "b", "c", "e", "d"}

	// prepare data
	for _, repoId := range repos {
		testutil.Must(t, r.CreateRepository(context.Background(), repoId, rocks.Repository{
			StorageNamespace: "s3://foo",
			CreationDate:     time.Now(),
			DefaultBranchID:  "master",
		}, rocks.Branch{}))
	}

	t.Run("listing all repos", func(t *testing.T) {
		iter := rocks.NewRepositoryIterator(context.Background(), db, 3, "")
		repoIds := make([]rocks.RepositoryID, 0)
		for iter.Next() {
			repo := iter.Value()
			repoIds = append(repoIds, repo.RepositoryID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if !reflect.DeepEqual(repoIds, []rocks.RepositoryID{"a", "aa", "b", "c", "d", "e"}) {
			t.Fatalf("got wrong list of repo IDs")
		}
	})

	t.Run("listing repos from prefix", func(t *testing.T) {
		iter := rocks.NewRepositoryIterator(context.Background(), db, 3, "b")
		repoIds := make([]rocks.RepositoryID, 0)
		for iter.Next() {
			repo := iter.Value()
			repoIds = append(repoIds, repo.RepositoryID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if !reflect.DeepEqual(repoIds, []rocks.RepositoryID{"b", "c", "d", "e"}) {
			t.Fatalf("got wrong list of repo IDs")
		}
	})

	t.Run("listing repos SeekGE", func(t *testing.T) {
		iter := rocks.NewRepositoryIterator(context.Background(), db, 3, "b")
		repoIds := make([]rocks.RepositoryID, 0)
		for iter.Next() {
			repo := iter.Value()
			repoIds = append(repoIds, repo.RepositoryID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if !reflect.DeepEqual(repoIds, []rocks.RepositoryID{"b", "c", "d", "e"}) {
			t.Fatalf("got wrong list of repo IDs")
		}

		// now let's seek
		if !iter.SeekGE("aa") {
			t.Fatalf("we should have values here")
		}

		repoIds = make([]rocks.RepositoryID, 0)
		for iter.Next() {
			repo := iter.Value()
			repoIds = append(repoIds, repo.RepositoryID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if !reflect.DeepEqual(repoIds, []rocks.RepositoryID{"aa", "b", "c", "d", "e"}) {
			t.Fatalf("got wrong list of repo IDs")
		}
	})
}

func TestPGBranchIterator(t *testing.T) {
	r, db := testRefManagerWithDB(t)
	branches := []rocks.BranchID{"a", "aa", "b", "c", "e", "d"}
	testutil.Must(t, r.CreateRepository(context.Background(), "repo1", rocks.Repository{
		StorageNamespace: "s3://foo",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, rocks.Branch{}))

	// prepare data
	for _, b := range branches {
		testutil.Must(t, r.SetBranch(context.Background(), "repo1", b, rocks.Branch{CommitID: "c1"}))
	}

	t.Run("listing all branches", func(t *testing.T) {
		iter := rocks.NewBranchIterator(context.Background(), db, "repo1", 3, "")
		ids := make([]rocks.BranchID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.BranchID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if !reflect.DeepEqual(ids, []rocks.BranchID{"a", "aa", "b", "c", "d", "e", "master"}) {
			t.Fatalf("got wrong list of IDs")
		}
	})

	t.Run("listing branches from prefix", func(t *testing.T) {
		iter := rocks.NewBranchIterator(context.Background(), db, "repo1", 3, "b")
		ids := make([]rocks.BranchID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.BranchID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if !reflect.DeepEqual(ids, []rocks.BranchID{"b", "c", "d", "e", "master"}) {
			t.Fatalf("got wrong list of branch IDs")
		}
	})

	t.Run("listing branches SeekGE", func(t *testing.T) {
		iter := rocks.NewBranchIterator(context.Background(), db, "repo1", 3, "b")
		ids := make([]rocks.BranchID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.BranchID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if !reflect.DeepEqual(ids, []rocks.BranchID{"b", "c", "d", "e", "master"}) {
			t.Fatalf("got wrong list of branch IDs")
		}

		// now let's seek
		if !iter.SeekGE("aa") {
			t.Fatalf("we should have values here")
		}

		ids = make([]rocks.BranchID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.BranchID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if !reflect.DeepEqual(ids, []rocks.BranchID{"aa", "b", "c", "d", "e", "master"}) {
			t.Fatalf("got wrong list of branch IDs")
		}
	})
}
