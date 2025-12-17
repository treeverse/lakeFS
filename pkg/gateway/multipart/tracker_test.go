package multipart_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/gateway/multipart"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/kv/mem"
)

func TestTrackerList(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	store, err := kv.Open(ctx, kvparams.Config{Type: mem.DriverName})
	require.NoError(t, err)
	defer store.Close()

	tracker := multipart.NewTracker(store)

	// Test empty list
	iter, err := tracker.List(ctx)
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
		err := tracker.Create(ctx, upload)
		require.NoError(t, err)
	}

	// List all uploads using iterator
	iter, err = tracker.List(ctx)
	require.NoError(t, err)
	defer iter.Close()

	// Collect uploads from iterator
	var uploads []*multipart.Upload
	for iter.Next() {
		upload := iter.Value()
		require.NotNil(t, upload)
		uploads = append(uploads, upload)
	}
	require.NoError(t, iter.Err())
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

	// Test that List returns independent iterators
	sort.Slice(uploads, func(i, j int) bool {
		return uploads[i].Path < uploads[j].Path
	})

	// List again to verify we can iterate multiple times
	iter2, err := tracker.List(ctx)
	require.NoError(t, err)
	defer iter2.Close()

	count := 0
	for iter2.Next() {
		count++
	}
	require.NoError(t, iter2.Err())
	require.Equal(t, len(testUploads), count)
}

func TestTrackerListAfterDelete(t *testing.T) {
	ctx := context.Background()
	store, err := kv.Open(ctx, kvparams.Config{Type: mem.DriverName})
	require.NoError(t, err)
	defer store.Close()

	tracker := multipart.NewTracker(store)
	const uploadsNum = 5
	// Create uploads
	for i := 0; i < uploadsNum; i++ {
		err := tracker.Create(ctx, multipart.Upload{
			UploadID:        fmt.Sprintf("upload-%d", i),
			Path:            fmt.Sprintf("path-%d", i),
			CreationDate:    time.Now(),
			PhysicalAddress: fmt.Sprintf("addr-%d", i),
		})
		require.NoError(t, err)
	}

	// List before delete
	iter, err := tracker.List(ctx)
	require.NoError(t, err)

	count := 0
	for iter.Next() {
		count++
	}
	iter.Close()
	require.NoError(t, iter.Err())
	require.Equal(t, uploadsNum, count)

	// Delete one upload
	err = tracker.Delete(ctx, "upload-2")
	require.NoError(t, err)

	// List after delete
	iter, err = tracker.List(ctx)
	require.NoError(t, err)
	defer iter.Close()

	var uploads []*multipart.Upload
	for iter.Next() {
		uploads = append(uploads, iter.Value())
	}
	require.NoError(t, iter.Err())
	require.Len(t, uploads, uploadsNum-1)

	// Verify deleted upload is not in the list
	for _, upload := range uploads {
		require.NotEqual(t, "upload-2", upload.UploadID)
	}
}

func TestTrackerListSeekGE(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
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
		err := tracker.Create(ctx, upload)
		require.NoError(t, err)
	}

	// Test 1: SeekGE to middle of list
	iter, err := tracker.List(ctx)
	require.NoError(t, err)
	defer iter.Close()

	iter.SeekGE("b-second", "upload-2")
	require.True(t, iter.Next(), "should have next after seeking")
	upload := iter.Value()
	require.Equal(t, "c-third", upload.Path, "should seek to first upload after b-second")

	// Test 2: SeekGE to exact match with same key, different upload ID
	iter2, err := tracker.List(ctx)
	require.NoError(t, err)
	defer iter2.Close()

	iter2.SeekGE("c-third", "upload-3a")
	require.True(t, iter2.Next(), "should have next after seeking")
	upload2 := iter2.Value()
	require.Equal(t, "c-third", upload2.Path)
	require.Equal(t, "upload-3b", upload2.UploadID, "should skip to next upload with same path")

	// Test 3: SeekGE to beginning
	iter3, err := tracker.List(ctx)
	require.NoError(t, err)
	defer iter3.Close()

	iter3.SeekGE("", "")
	require.True(t, iter3.Next(), "should have next from beginning")
	upload3 := iter3.Value()
	require.Equal(t, "a-first", upload3.Path, "should start from first upload")

	// Test 4: SeekGE beyond end
	iter4, err := tracker.List(ctx)
	require.NoError(t, err)
	defer iter4.Close()

	iter4.SeekGE("z-last", "")
	require.False(t, iter4.Next(), "should have no next when seeking beyond end")

	// Test 5: Pagination simulation - get first 2, then seek to continue
	iter5, err := tracker.List(ctx)
	require.NoError(t, err)
	defer iter5.Close()

	// Get first 2 uploads
	count := 0
	var lastPath, lastUploadID string
	for iter5.Next() && count < 2 {
		u := iter5.Value()
		lastPath = u.Path
		lastUploadID = u.UploadID
		count++
	}
	require.Equal(t, 2, count)
	require.Equal(t, "b-second", lastPath)

	// Now seek to continue from where we left off
	iter6, err := tracker.List(ctx)
	require.NoError(t, err)
	defer iter6.Close()

	iter6.SeekGE(lastPath, lastUploadID)
	require.True(t, iter6.Next(), "should have next after pagination seek")
	nextUpload := iter6.Value()
	require.Equal(t, "c-third", nextUpload.Path, "should continue from correct position")
}

func TestTrackerListSeekGE_AWSSpecCompliance(t *testing.T) {
	// This test verifies AWS S3 spec compliance for marker behavior:
	// "Items with keys equal to key-marker are included ONLY IF their upload ID
	// is lexicographically greater than upload-id-marker"
	// The marker itself (exact key + exact uploadID) should be EXCLUDED
	ctx := context.Background()
	now := time.Now()
	store, err := kv.Open(ctx, kvparams.Config{Type: mem.DriverName})
	require.NoError(t, err)
	defer store.Close()

	tracker := multipart.NewTracker(store)

	// Create uploads with specific keys and upload IDs to test exact marker exclusion
	testUploads := []multipart.Upload{
		{
			UploadID:        "upload-a-1",
			Path:            "key-alpha",
			CreationDate:    now,
			PhysicalAddress: "addr-a-1",
		},
		{
			UploadID:        "upload-a-2",
			Path:            "key-alpha", // Same key as above
			CreationDate:    now,
			PhysicalAddress: "addr-a-2",
		},
		{
			UploadID:        "upload-a-3",
			Path:            "key-alpha", // Same key as above
			CreationDate:    now,
			PhysicalAddress: "addr-a-3",
		},
		{
			UploadID:        "upload-b-1",
			Path:            "key-beta",
			CreationDate:    now,
			PhysicalAddress: "addr-b-1",
		},
	}

	for _, upload := range testUploads {
		err := tracker.Create(ctx, upload)
		require.NoError(t, err)
	}

	// Test 1: Seek with exact marker should EXCLUDE the marker itself
	iter, err := tracker.List(ctx)
	require.NoError(t, err)
	defer iter.Close()

	iter.SeekGE("key-alpha", "upload-a-2")
	require.True(t, iter.Next(), "should have next after seeking")
	upload := iter.Value()
	require.NotNil(t, upload)
	// Per AWS spec: The exact marker (key-alpha, upload-a-2) should be EXCLUDED
	require.NotEqual(t, "upload-a-2", upload.UploadID, "marker itself should be excluded")
	// Next item should be (key-alpha, upload-a-3) since upload-a-3 > upload-a-2
	require.Equal(t, "key-alpha", upload.Path)
	require.Equal(t, "upload-a-3", upload.UploadID, "should return next upload ID after marker")

	// Test 2: Seek to first upload of a key should work correctly
	iter2, err := tracker.List(ctx)
	require.NoError(t, err)
	defer iter2.Close()

	iter2.SeekGE("key-alpha", "upload-a-1")
	require.True(t, iter2.Next())
	upload2 := iter2.Value()
	require.NotNil(t, upload2)
	// Exact marker should be excluded
	require.NotEqual(t, "upload-a-1", upload2.UploadID, "marker itself should be excluded")
	require.Equal(t, "key-alpha", upload2.Path)
	require.Equal(t, "upload-a-2", upload2.UploadID)

	// Test 3: Seek to last upload of a key should move to next key
	iter3, err := tracker.List(ctx)
	require.NoError(t, err)
	defer iter3.Close()

	iter3.SeekGE("key-alpha", "upload-a-3")
	require.True(t, iter3.Next())
	upload3 := iter3.Value()
	require.NotNil(t, upload3)
	// Should skip to next key since upload-a-3 is the last for key-alpha
	require.Equal(t, "key-beta", upload3.Path, "should move to next key when seeking past last upload of current key")
	require.Equal(t, "upload-b-1", upload3.UploadID)
}
