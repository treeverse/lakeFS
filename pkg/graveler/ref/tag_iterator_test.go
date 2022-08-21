package ref_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestDBTagIterator(t *testing.T) {
	r, db := testRefManagerWithDB(t)
	tags := []graveler.TagID{"a", "aa", "b", "c", "e", "d", "f", "g"}
	ctx := context.Background()

	repository, err := r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageNamespace: "s3://foo",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)

	// prepare data
	for _, b := range tags {
		err := r.CreateTag(ctx, repository, b, "c1")
		testutil.Must(t, err)
	}

	t.Run("listing all tags", func(t *testing.T) {
		iter, err := ref.NewDBTagIterator(ctx, db, repository.RepositoryID, 3)
		testutil.Must(t, err)
		ids := make([]graveler.TagID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.TagID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(ids, []graveler.TagID{"a", "aa", "b", "c", "d", "e", "f", "g"}); diffs != nil {
			t.Fatalf("got wrong list of IDs: %v", diffs)
		}
	})

	t.Run("listing tags using prefix", func(t *testing.T) {
		iter, err := ref.NewDBTagIterator(ctx, db, repository.RepositoryID, 3)
		testutil.Must(t, err)
		iter.SeekGE("b")
		ids := make([]graveler.TagID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.TagID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(ids, []graveler.TagID{"b", "c", "d", "e", "f", "g"}); diffs != nil {
			t.Fatalf("got wrong list of tags: %v", diffs)
		}
	})

	t.Run("empty value SeekGE", func(t *testing.T) {
		iter, err := ref.NewDBTagIterator(ctx, db, repository.RepositoryID, 3)
		testutil.Must(t, err)
		iter.SeekGE("b")

		if iter.Value() != nil {
			t.Fatalf("expected nil value after seekGE")
		}
	})

	t.Run("listing tags SeekGE", func(t *testing.T) {
		iter, err := ref.NewDBTagIterator(ctx, db, repository.RepositoryID, 3)
		testutil.Must(t, err)
		iter.SeekGE("b")
		ids := make([]graveler.TagID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.TagID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}

		if diffs := deep.Equal(ids, []graveler.TagID{"b", "c", "d", "e", "f", "g"}); diffs != nil {
			t.Fatalf("got wrong list of tags: %v", diffs)
		}

		// now let's seek
		iter.SeekGE("aa")
		ids = make([]graveler.TagID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.TagID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(ids, []graveler.TagID{"aa", "b", "c", "d", "e", "f", "g"}); diffs != nil {
			t.Fatalf("got wrong list of tags")
		}
	})
}

func TestKVTagIterator(t *testing.T) {
	r, kvstore := testRefManagerWithKV(t)
	tags := []graveler.TagID{"a", "aa", "b", "c", "e", "d", "f", "g"}
	ctx := context.Background()
	repository, err := r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageNamespace: "s3://foo",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)

	// prepare data
	for _, b := range tags {
		err := r.CreateTag(ctx, repository, b, "c1")
		testutil.Must(t, err)
	}

	t.Run("listing all tags", func(t *testing.T) {
		iter, err := ref.NewKVTagIterator(ctx, &kvstore, repository)
		testutil.Must(t, err)
		ids := make([]graveler.TagID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.TagID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(ids, []graveler.TagID{"a", "aa", "b", "c", "d", "e", "f", "g"}); diffs != nil {
			t.Fatalf("got wrong list of IDs: %v", diffs)
		}
	})

	t.Run("listing tags using prefix", func(t *testing.T) {
		iter, err := ref.NewKVTagIterator(ctx, &kvstore, repository)
		testutil.Must(t, err)
		iter.SeekGE("b")
		ids := make([]graveler.TagID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.TagID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(ids, []graveler.TagID{"b", "c", "d", "e", "f", "g"}); diffs != nil {
			t.Fatalf("got wrong list of tags: %v", diffs)
		}
	})

	t.Run("listing tags SeekGE", func(t *testing.T) {
		iter, err := ref.NewKVTagIterator(ctx, &kvstore, repository)
		testutil.Must(t, err)
		iter.SeekGE("b")
		ids := make([]graveler.TagID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.TagID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}

		if diffs := deep.Equal(ids, []graveler.TagID{"b", "c", "d", "e", "f", "g"}); diffs != nil {
			t.Fatalf("got wrong list of tags: %v", diffs)
		}

		// now let's seek
		iter.SeekGE("aa")
		ids = make([]graveler.TagID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.TagID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(ids, []graveler.TagID{"aa", "b", "c", "d", "e", "f", "g"}); diffs != nil {
			t.Fatalf("got wrong list of tags")
		}
	})

	t.Run("empty value SeekGE", func(t *testing.T) {
		iter, err := ref.NewKVTagIterator(ctx, &kvstore, repository)
		testutil.Must(t, err)
		iter.SeekGE("b")

		if iter.Value() != nil {
			t.Fatalf("expected nil value after seekGE")
		}
	})
}
