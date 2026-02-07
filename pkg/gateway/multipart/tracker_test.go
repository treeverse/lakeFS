package multipart_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/gateway/multipart"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/kv/mem"
)

func TestTrackerList(t *testing.T) {
	ctx := t.Context()
	now := time.Now()
	const testPartition = "test-repo-instance-123"
	store, err := kv.Open(ctx, kvparams.Config{Type: mem.DriverName})
	require.NoError(t, err)
	defer store.Close()

	tracker := multipart.NewTracker(store)

	// Test empty list
	iter, err := tracker.List(ctx, testPartition)
	require.NoError(t, err)
	defer iter.Close()

	require.False(t, iter.Next(), "empty iterator should not have next")
	require.NoError(t, iter.Err())

	// Create multiple uploads with different logical paths
	testUploads := []multipart.Upload{
		{
			UploadID:        "upload-3",
			Path:            "foo2",
			CreationDate:    now,
			PhysicalAddress: "physical-addr-a", // Physical address doesn't match logical order
		},
		{
			UploadID:        "upload-1",
			Path:            "foo1",
			CreationDate:    now,
			PhysicalAddress: "physical-addr-c",
		},
		{
			UploadID:        "upload-2",
			Path:            "bar",
			CreationDate:    now,
			PhysicalAddress: "physical-addr-b",
		},
		{
			UploadID:        "upload-4",
			Path:            "foo1", // Same path as upload-1, different upload ID
			CreationDate:    now.Add(time.Second),
			PhysicalAddress: "physical-addr-d",
		},
	}

	// Create all uploads
	for _, upload := range testUploads {
		err := tracker.Create(ctx, testPartition, upload)
		require.NoError(t, err)
	}

	// List all uploads using iterator
	iter2, err := tracker.List(ctx, testPartition)
	require.NoError(t, err)
	defer iter2.Close()

	// Collect uploads from iterator
	var uploads []*multipart.Upload
	for iter2.Next() {
		upload := iter2.Value()
		uploads = append(uploads, upload)
	}
	require.NoError(t, iter2.Err())
	require.Len(t, uploads, len(testUploads))

	// Verify all uploads are present
	uploadMap := make(map[string]*multipart.Upload)
	for _, upload := range uploads {
		uploadMap[upload.UploadID] = upload
	}

	for _, expected := range testUploads {
		actual, found := uploadMap[expected.UploadID]
		require.True(t, found, "upload %s not found", expected.UploadID)
		require.Equal(t, expected.Path, actual.Path)
		require.Equal(t, expected.PhysicalAddress, actual.PhysicalAddress)
	}

	// Verify list is sorted
	for i := range len(uploads) - 1 {
		require.LessOrEqual(t, uploads[i].Path, uploads[i+1].Path)
		if uploads[i].Path == uploads[i+1].Path {
			require.Less(t, uploads[i].UploadID, uploads[i+1].UploadID)
		}
	}

	// List again to verify we can iterate multiple times
	iter3, err := tracker.List(ctx, testPartition)
	require.NoError(t, err)
	defer iter3.Close()

	count := 0
	for iter3.Next() {
		count++
	}
	require.NoError(t, iter3.Err())
	require.Equal(t, len(testUploads), count)
}

func TestTrackerListAfterDelete(t *testing.T) {
	ctx := t.Context()
	const testPartition = "test-repo-instance-123"
	store, err := kv.Open(ctx, kvparams.Config{Type: mem.DriverName})
	require.NoError(t, err)
	defer store.Close()

	tracker := multipart.NewTracker(store)
	const uploadsNum = 5
	// Create uploads
	for i := 0; i < uploadsNum; i++ {
		err := tracker.Create(ctx, testPartition, multipart.Upload{
			UploadID:        fmt.Sprintf("upload-%d", i),
			Path:            fmt.Sprintf("path-%d", i),
			CreationDate:    time.Now(),
			PhysicalAddress: fmt.Sprintf("addr-%d", i),
		})
		require.NoError(t, err)
	}

	// List before delete
	iter, err := tracker.List(ctx, testPartition)
	require.NoError(t, err)
	defer iter.Close()

	count := 0
	for iter.Next() {
		count++
	}
	require.NoError(t, iter.Err())
	require.Equal(t, uploadsNum, count)

	// Delete one upload
	err = tracker.Delete(ctx, testPartition, "path-2", "upload-2")
	require.NoError(t, err)

	// List after delete
	iter2, err := tracker.List(ctx, testPartition)
	require.NoError(t, err)
	defer iter2.Close()

	var uploads []string
	for iter2.Next() {
		uploads = append(uploads, iter2.Value().UploadID)
	}
	require.NoError(t, iter2.Err())
	require.Len(t, uploads, uploadsNum-1)

	// Verify deleted upload is not in the list
	require.NotContains(t, uploads, "upload-2")
}

func TestTrackerListSeekGE(t *testing.T) {
	ctx := t.Context()
	now := time.Now()
	const testPartition = "test-repo-instance-123"
	store, err := kv.Open(ctx, kvparams.Config{Type: mem.DriverName})
	require.NoError(t, err)
	defer store.Close()

	tracker := multipart.NewTracker(store)

	// Create uploads with specific paths for testing seeking
	testUploads := []multipart.Upload{
		{
			UploadID:        "upload-1",
			Path:            "a-first",
			CreationDate:    now,
			PhysicalAddress: "addr-1",
		},
		{
			UploadID:        "upload-2",
			Path:            "b-second",
			CreationDate:    now,
			PhysicalAddress: "addr-2",
		},
		{
			UploadID:        "upload-3a",
			Path:            "c-third",
			CreationDate:    now,
			PhysicalAddress: "addr-3a",
		},
		{
			UploadID:        "upload-3b",
			Path:            "c-third", // Same path as upload-3a
			CreationDate:    now.Add(time.Second),
			PhysicalAddress: "addr-3b",
		},
		{
			UploadID:        "upload-4",
			Path:            "d-fourth",
			CreationDate:    now,
			PhysicalAddress: "addr-4",
		},
	}

	// Create all uploads
	for _, upload := range testUploads {
		err := tracker.Create(ctx, testPartition, upload)
		require.NoError(t, err)
	}

	// Test 1: SeekGE to exact match returns that item or next
	iter, err := tracker.List(ctx, testPartition)
	require.NoError(t, err)
	defer iter.Close()

	iter.SeekGE("b-second", "upload-2")
	require.True(t, iter.Next(), "should have next after seeking")
	upload := iter.Value()
	// Should return >= "b-second/upload-2"
	require.True(t, upload.Path >= "b-second", "should seek to position >= marker")

	// Test 2: SeekGE to exact match with same key
	iter2, err := tracker.List(ctx, testPartition)
	require.NoError(t, err)
	defer iter2.Close()

	iter2.SeekGE("c-third", "upload-3a")
	require.True(t, iter2.Next(), "should have next after seeking")
	upload2 := iter2.Value()
	// Should return >= "c-third/upload-3a"
	require.True(t, upload2.Path >= "c-third", "should seek to position >= marker")
	if upload2.Path == "c-third" {
		require.True(t, upload2.UploadID >= "upload-3a", "should return >= marker uploadID")
	}

	// Test 3: SeekGE to beginning
	iter3, err := tracker.List(ctx, testPartition)
	require.NoError(t, err)
	defer iter3.Close()

	iter3.SeekGE("", "")
	require.True(t, iter3.Next(), "should have next from beginning")
	upload3 := iter3.Value()
	require.Equal(t, "a-first", upload3.Path, "should start from first upload")

	// Test 4: SeekGE beyond end
	iter4, err := tracker.List(ctx, testPartition)
	require.NoError(t, err)
	defer iter4.Close()

	iter4.SeekGE("z-last", "")
	require.False(t, iter4.Next(), "should have no next when seeking beyond end")
}
