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

func TestAddressTokenIterator(t *testing.T) {
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
		err := r.SetAddressToken(ctx, repository, b)
		testutil.Must(t, err)
	}

	t.Run("listing all addresses", func(t *testing.T) {
		iter, err := ref.NewAddressTokenIterator(ctx, kvstore, repository)
		testutil.Must(t, err)
		ids := make([]string, 0)
		for iter.Next() {
			b := iter.Value()
			ids = append(ids, b.Address)
		}
		if iter.Err() != nil {
			t.Fatalf("unexpected error: %v", iter.Err())
		}
		iter.Close()

		if diffs := deep.Equal(ids, addresses); diffs != nil {
			t.Fatalf("got wrong list of IDs: %v", diffs)
		}
	})

	//t.Run("listing tags using prefix", func(t *testing.T) {
	//	iter, err := ref.NewKVTagIterator(ctx, kvstore, repository)
	//	testutil.Must(t, err)
	//	iter.SeekGE("b")
	//	ids := make([]graveler.TagID, 0)
	//	for iter.Next() {
	//		b := iter.Value()
	//		ids = append(ids, b.TagID)
	//	}
	//	if iter.Err() != nil {
	//		t.Fatalf("unexpected error: %v", iter.Err())
	//	}
	//	iter.Close()
	//
	//	if diffs := deep.Equal(ids, []graveler.TagID{"b", "c", "d", "e", "f", "g"}); diffs != nil {
	//		t.Fatalf("got wrong list of tags: %v", diffs)
	//	}
	//})
	//
	//t.Run("listing tags SeekGE", func(t *testing.T) {
	//	iter, err := ref.NewKVTagIterator(ctx, kvstore, repository)
	//	testutil.Must(t, err)
	//	iter.SeekGE("b")
	//	ids := make([]graveler.TagID, 0)
	//	for iter.Next() {
	//		b := iter.Value()
	//		ids = append(ids, b.TagID)
	//	}
	//	if iter.Err() != nil {
	//		t.Fatalf("unexpected error: %v", iter.Err())
	//	}
	//
	//	if diffs := deep.Equal(ids, []graveler.TagID{"b", "c", "d", "e", "f", "g"}); diffs != nil {
	//		t.Fatalf("got wrong list of tags: %v", diffs)
	//	}
	//
	//	// now let's seek
	//	iter.SeekGE("aa")
	//	ids = make([]graveler.TagID, 0)
	//	for iter.Next() {
	//		b := iter.Value()
	//		ids = append(ids, b.TagID)
	//	}
	//	if iter.Err() != nil {
	//		t.Fatalf("unexpected error: %v", iter.Err())
	//	}
	//	iter.Close()
	//
	//	if diffs := deep.Equal(ids, []graveler.TagID{"aa", "b", "c", "d", "e", "f", "g"}); diffs != nil {
	//		t.Fatalf("got wrong list of tags")
	//	}
	//})
	//
	//t.Run("empty value SeekGE", func(t *testing.T) {
	//	iter, err := ref.NewKVTagIterator(ctx, kvstore, repository)
	//	testutil.Must(t, err)
	//	iter.SeekGE("b")
	//
	//	if iter.Value() != nil {
	//		t.Fatalf("expected nil value after seekGE")
	//	}
	//})
}

//func TestKVTagIterator_CloseTwice(t *testing.T) {
//	ctx := context.Background()
//	ctrl := gomock.NewController(t)
//	entIt := mock.NewMockEntriesIterator(ctrl)
//	entIt.EXPECT().Close().Times(1)
//	store := mock.NewMockStore(ctrl)
//	store.EXPECT().Scan(ctx, gomock.Any(), gomock.Any()).Return(entIt, nil).Times(1)
//	msgStore := kv.StoreMessage{Store: store}
//	repo := &graveler.RepositoryRecord{
//		RepositoryID: "repo",
//		Repository: &graveler.Repository{
//			InstanceUID: "rid",
//		},
//	}
//	it, err := ref.NewKVTagIterator(ctx, &msgStore, repo)
//	if err != nil {
//		t.Fatal("TestKVTagIterator failed", err)
//	}
//	it.Close()
//	// Make sure calling Close again do not crash
//	it.Close()
//}
//
//func TestKVTagIterator_NextClosed(t *testing.T) {
//	ctx := context.Background()
//	ctrl := gomock.NewController(t)
//	entIt := mock.NewMockEntriesIterator(ctrl)
//	entIt.EXPECT().Close().Times(1)
//	store := mock.NewMockStore(ctrl)
//	store.EXPECT().Scan(ctx, gomock.Any(), gomock.Any()).Return(entIt, nil).Times(1)
//	msgStore := kv.StoreMessage{Store: store}
//	repo := &graveler.RepositoryRecord{
//		RepositoryID: "repo",
//		Repository: &graveler.Repository{
//			InstanceUID: "rid",
//		},
//	}
//	it, err := ref.NewKVTagIterator(ctx, &msgStore, repo)
//	if err != nil {
//		t.Fatal("TestKVTagIterator failed", err)
//	}
//	it.Close()
//	// Make sure calling Next should not crash
//	it.Next()
//}
