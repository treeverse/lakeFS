package ref_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/mock"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestTagIterator(t *testing.T) {
	r, kvstore := testRefManager(t)
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
		iter, err := ref.NewTagIterator(ctx, kvstore, repository)
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
		iter, err := ref.NewTagIterator(ctx, kvstore, repository)
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
		iter, err := ref.NewTagIterator(ctx, kvstore, repository)
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
		iter, err := ref.NewTagIterator(ctx, kvstore, repository)
		testutil.Must(t, err)
		iter.SeekGE("b")

		if iter.Value() != nil {
			t.Fatalf("expected nil value after seekGE")
		}
	})
}

func TestTagIterator_CloseTwice(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	entIt := mock.NewMockEntriesIterator(ctrl)
	entIt.EXPECT().Close().Times(1)
	store := mock.NewMockStore(ctrl)
	store.EXPECT().Scan(ctx, gomock.Any(), gomock.Any()).Return(entIt, nil).Times(1)
	msgStore := kv.StoreMessage{Store: store}
	repo := &graveler.RepositoryRecord{
		RepositoryID: "repo",
		Repository: &graveler.Repository{
			InstanceUID: "rid",
		},
	}
	it, err := ref.NewTagIterator(ctx, &msgStore, repo)
	if err != nil {
		t.Fatal("TestTagIterator failed", err)
	}
	it.Close()
	// Make sure calling Close again do not crash
	it.Close()
}

func TestTagIterator_NextClosed(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	entIt := mock.NewMockEntriesIterator(ctrl)
	entIt.EXPECT().Close().Times(1)
	store := mock.NewMockStore(ctrl)
	store.EXPECT().Scan(ctx, gomock.Any(), gomock.Any()).Return(entIt, nil).Times(1)
	msgStore := kv.StoreMessage{Store: store}
	repo := &graveler.RepositoryRecord{
		RepositoryID: "repo",
		Repository: &graveler.Repository{
			InstanceUID: "rid",
		},
	}
	it, err := ref.NewTagIterator(ctx, &msgStore, repo)
	if err != nil {
		t.Fatal("TestTagIterator failed", err)
	}
	it.Close()
	// Make sure calling Next should not crash
	it.Next()
}
