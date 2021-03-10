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

func TestBranchIterator(t *testing.T) {
	r, db := testRefManagerWithDB(t)
	branches := []graveler.BranchID{"a", "aa", "b", "c", "e", "d"}
	ctx := context.Background()
	testutil.Must(t, r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageNamespace: "s3://foo",
		CreationDate:     time.Now(),
		DefaultBranchID:  "master",
	}, ""))

	// prepare data
	for _, b := range branches {
		testutil.Must(t, r.SetBranch(ctx, "repo1", b, graveler.Branch{CommitID: "c1"}))
	}

	t.Run("listing all branches", func(t *testing.T) {
		iter := ref.NewBranchIterator(ctx, db, "repo1", 3)
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

	t.Run("listing branches using prefix", func(t *testing.T) {
		iter := ref.NewBranchIterator(ctx, db, "repo1", 3)
		iter.SeekGE("b")
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
		iter := ref.NewBranchIterator(ctx, db, "repo1", 3)
		iter.SeekGE("b")
		ids := make([]graveler.BranchID, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.BranchID)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}

		if diffs := deep.Equal(ids, []graveler.BranchID{"b", "c", "d", "e", "master"}); diffs != nil {
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

		if diffs := deep.Equal(ids, []graveler.BranchID{"aa", "b", "c", "d", "e", "master"}); diffs != nil {
			t.Fatalf("got wrong list of branch IDs")
		}
	})
}
