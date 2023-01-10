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

func TestLinkAddressIterator(t *testing.T) {
	r, kvstore := testRefManager(t)
	addresses := []string{"data/a/a", "data/a/b", "data/a/c", "data/a/d", "data/a/e", "data/a/f", "data/a/g"}
	ctx := context.Background()
	repository, err := r.CreateRepository(ctx, "repo1", graveler.Repository{
		StorageNamespace: "s3://foo",
		CreationDate:     time.Now(),
		DefaultBranchID:  "main",
	})
	testutil.Must(t, err)

	// prepare data
	for _, b := range addresses {
		err := r.SetLinkAddress(ctx, repository, b)
		testutil.Must(t, err)
	}

	t.Run("listing all addresses", func(t *testing.T) {
		iter, err := ref.NewAddressTokenIterator(ctx, kvstore, repository)
		testutil.Must(t, err)
		ids := make([]string, 0)
		for iter.Next() {
			a := iter.Value()
			ids = append(ids, a.Address)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(ids, addresses); diffs != nil {
			t.Fatalf("got wrong list of addresses: %v", diffs)
		}
	})

	t.Run("listing addresses SeekGE", func(t *testing.T) {
		iter, err := ref.NewAddressTokenIterator(ctx, kvstore, repository)
		testutil.Must(t, err)
		iter.SeekGE("data/a/e")
		ids := make([]string, 0)
		for iter.Next() {
			a := iter.Value()
			ids = append(ids, a.Address)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(ids, []string{"data/a/e", "data/a/f", "data/a/g"}); diffs != nil {
			t.Fatalf("got wrong list of addresses: %v", diffs)
		}
	})

	t.Run("empty value SeekGE", func(t *testing.T) {
		iter, err := ref.NewAddressTokenIterator(ctx, kvstore, repository)
		testutil.Must(t, err)
		iter.SeekGE("data/b")

		if iter.Value() != nil {
			t.Fatalf("expected nil value after seekGE")
		}
	})
}
