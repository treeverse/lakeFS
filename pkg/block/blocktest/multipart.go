package blocktest

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/pkg/block"
)

const (
	multipartNumberOfParts = 3
	multipartPartSize      = 5 * 1024 * 1024 // generally the minimum size for multi-part upload
)

// Parameterized test of the Multipart Upload APIs. After successful upload we Get the result and compare to the original
func testAdapterMultipartUpload(t *testing.T, adapter block.Adapter, storageNamespace string) {
	ctx := context.Background()
	cases := []struct {
		name            string
		path            string
		lastPartPartial bool
	}{
		{"simple", "abc", false},
		{"partial", "abc", true},
		{"nested", "foo/bar", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			blockstoreType := adapter.BlockstoreType()
			obj := block.ObjectPointer{
				StorageNamespace: storageNamespace,
				Identifier:       c.path,
				IdentifierType:   block.IdentifierTypeRelative,
			}
			parts, full := createMultipartContents(c.lastPartPartial)

			verifyListInvalid(t, ctx, adapter, obj, "invalidId")

			resp, err := adapter.CreateMultiPartUpload(ctx, obj, nil, block.CreateMultiPartUploadOpts{})
			require.NoError(t, err)

			multiParts := uploadParts(t, ctx, adapter, obj, resp.UploadID, parts)

			// List parts after upload
			listResp, err := adapter.ListParts(ctx, obj, resp.UploadID, block.ListPartsOpts{})
			if blockstoreType != block.BlockstoreTypeS3 {
				require.ErrorIs(t, err, block.ErrOperationNotSupported)
			} else {
				require.NoError(t, err)
				require.Equal(t, len(parts), len(listResp.Parts))
				for i, part := range listResp.Parts {
					require.Equal(t, multiParts[i].PartNumber, part.PartNumber)
					require.Equal(t, int64(len(parts[i])), part.Size)
					require.Equal(t, multiParts[i].ETag, part.ETag)
					require.False(t, listResp.IsTruncated)
				}
			}

			// List parts partial
			const maxPartsConst = 2
			maxParts := int32(maxPartsConst)
			listResp, err = adapter.ListParts(ctx, obj, resp.UploadID, block.ListPartsOpts{MaxParts: &maxParts})
			if blockstoreType != block.BlockstoreTypeS3 {
				require.ErrorIs(t, err, block.ErrOperationNotSupported)
			} else {
				require.NoError(t, err)
				require.Equal(t, int(maxParts), len(listResp.Parts))
				require.True(t, listResp.IsTruncated)
				require.Equal(t, strconv.Itoa(int(maxParts)), *listResp.NextPartNumberMarker)
			}

			_, err = adapter.CompleteMultiPartUpload(ctx, obj, resp.UploadID, &block.MultipartUploadCompletion{
				Part: multiParts,
			})
			require.NoError(t, err)

			verifyListInvalid(t, ctx, adapter, obj, resp.UploadID)

			getAndCheckContents(t, ctx, adapter, full, obj)
		})
	}
}

// Test aborting a multipart upload
func testAdapterAbortMultipartUpload(t *testing.T, adapter block.Adapter, storageNamespace string) {
	ctx := context.Background()
	parts, _ := createMultipartContents(false)
	obj, _ := objPointers(storageNamespace)

	// Start an upload
	resp, err := adapter.CreateMultiPartUpload(ctx, obj, nil, block.CreateMultiPartUploadOpts{})
	require.NoError(t, err)
	uploadID := resp.UploadID

	uploadParts(t, ctx, adapter, obj, uploadID, parts)

	listResp, err := adapter.ListParts(ctx, obj, uploadID, block.ListPartsOpts{})
	if err == nil {
		require.Equal(t, multipartNumberOfParts, len(listResp.Parts))
	}

	// Test abort
	err = adapter.AbortMultiPartUpload(ctx, obj, uploadID)
	require.NoError(t, err)

	// verify no parts to list
	verifyListInvalid(t, ctx, adapter, obj, uploadID)
}

// Test of the Multipart Copy API
func testAdapterCopyPart(t *testing.T, adapter block.Adapter, storageNamespace string) {
	ctx := context.Background()
	parts, full := createMultipartContents(false)
	obj, objCopy := objPointers(storageNamespace)
	uploadMultiPart(t, ctx, adapter, obj, parts)

	// Begin Multipart Copy
	resp, err := adapter.CreateMultiPartUpload(ctx, objCopy, nil, block.CreateMultiPartUploadOpts{})
	require.NoError(t, err)
	multiParts, err := copyPart(t, ctx, adapter, obj, objCopy, resp.UploadID)
	if adapter.BlockstoreType() == block.BlockstoreTypeAzure {
		require.ErrorContains(t, err, "not implemented") // azurite block store emulator did not yet implement this
		return
	}
	_, err = adapter.CompleteMultiPartUpload(ctx, objCopy, resp.UploadID, &block.MultipartUploadCompletion{
		Part: multiParts,
	})
	require.NoError(t, err)

	getAndCheckContents(t, ctx, adapter, full, objCopy)
}

// Test of the Multipart CopyRange API
func testAdapterCopyPartRange(t *testing.T, adapter block.Adapter, storageNamespace string) {
	ctx := context.Background()
	parts, full := createMultipartContents(false)
	obj, objCopy := objPointers(storageNamespace)
	uploadMultiPart(t, ctx, adapter, obj, parts)

	// Begin Multipart Copy
	resp, err := adapter.CreateMultiPartUpload(ctx, objCopy, nil, block.CreateMultiPartUploadOpts{})
	require.NoError(t, err)
	multiParts := copyPartRange(t, ctx, adapter, obj, objCopy, resp.UploadID)
	if adapter.BlockstoreType() == block.BlockstoreTypeAzure {
		// not implemented in Azure
		require.Nil(t, multiParts) // azurite block store emulator did not yet implement this
		return
	}

	_, err = adapter.CompleteMultiPartUpload(ctx, objCopy, resp.UploadID, &block.MultipartUploadCompletion{
		Part: multiParts,
	})
	require.NoError(t, err)

	getAndCheckContents(t, ctx, adapter, full, objCopy)
}

func createMultipartContents(lastPartPartial bool) ([][]byte, []byte) {
	parts := make([][]byte, multipartNumberOfParts)
	var partsConcat []byte
	for i := 0; i < multipartNumberOfParts; i++ {
		if lastPartPartial && (i == multipartNumberOfParts-1) {
			parts[i] = randstr.Bytes(multipartPartSize/2 + i)
		} else {
			parts[i] = randstr.Bytes(multipartPartSize + i)
		}
		partsConcat = append(partsConcat, parts[i]...)
	}
	return parts, partsConcat
}

func uploadMultiPart(t *testing.T, ctx context.Context, adapter block.Adapter, obj block.ObjectPointer, parts [][]byte) {
	t.Helper()
	resp, err := adapter.CreateMultiPartUpload(ctx, obj, nil, block.CreateMultiPartUploadOpts{})
	require.NoError(t, err)

	multiParts := uploadParts(t, ctx, adapter, obj, resp.UploadID, parts)

	_, err = adapter.CompleteMultiPartUpload(ctx, obj, resp.UploadID, &block.MultipartUploadCompletion{
		Part: multiParts,
	})
	require.NoError(t, err)
}

// List parts when there are none
func verifyListInvalid(t *testing.T, ctx context.Context, adapter block.Adapter, obj block.ObjectPointer, uploadID string) {
	t.Helper()
	_, err := adapter.ListParts(ctx, obj, uploadID, block.ListPartsOpts{})
	if adapter.BlockstoreType() != block.BlockstoreTypeS3 {
		require.ErrorIs(t, err, block.ErrOperationNotSupported)
	} else {
		require.NotNil(t, err)
	}
}

// Upload parts after starting a multipart upload
func uploadParts(t *testing.T, ctx context.Context, adapter block.Adapter, obj block.ObjectPointer, uploadID string, parts [][]byte) []block.MultipartPart {
	t.Helper()
	multiParts := make([]block.MultipartPart, len(parts))
	for i, content := range parts {
		partNumber := i + 1
		partResp, err := adapter.UploadPart(ctx, obj, int64(len(content)), bytes.NewReader(content), uploadID, partNumber)
		require.NoError(t, err)
		multiParts[i].PartNumber = partNumber
		multiParts[i].ETag = partResp.ETag
	}

	return multiParts
}

// Copy parts after starting a multipart upload
func copyPartRange(t *testing.T, ctx context.Context, adapter block.Adapter, obj, objCopy block.ObjectPointer, uploadID string) []block.MultipartPart {
	t.Helper()
	multiParts := make([]block.MultipartPart, multipartNumberOfParts)
	var startPosition int64 = 0
	for i := 0; i < multipartNumberOfParts; i++ {
		partNumber := i + 1
		var endPosition = startPosition + multipartPartSize
		partResp, err := adapter.UploadCopyPartRange(ctx, obj, objCopy, uploadID, partNumber, startPosition, endPosition)
		if adapter.BlockstoreType() == block.BlockstoreTypeAzure {
			require.ErrorContains(t, err, "not implemented") // azurite block store emulator did not yet implement this
			return nil
		}
		require.NoError(t, err)
		multiParts[i].PartNumber = partNumber
		multiParts[i].ETag = partResp.ETag
		startPosition = endPosition + 1
	}

	return multiParts
}

// Copy one and only part after starting a multipart upload
func copyPart(t *testing.T, ctx context.Context, adapter block.Adapter, obj, objCopy block.ObjectPointer, uploadID string) ([]block.MultipartPart, error) {
	t.Helper()
	multiParts := make([]block.MultipartPart, 1)
	partNumber := 1
	partResp, err := adapter.UploadCopyPart(ctx, obj, objCopy, uploadID, partNumber)
	if err != nil {
		return nil, err
	}
	multiParts[0].PartNumber = partNumber
	multiParts[0].ETag = partResp.ETag

	return multiParts, nil
}

// getAndCheckContents GETs the Object from the Store, then compares its contents to the exp byte array
func getAndCheckContents(t *testing.T, ctx context.Context, adapter block.Adapter, exp []byte, obj block.ObjectPointer) {
	t.Helper()
	// first check exists
	ok, err := adapter.Exists(ctx, obj)
	require.NoError(t, err, "Exists failed")
	require.True(t, ok, "Returned Non-OK Status")

	reader, err := adapter.Get(ctx, obj)
	require.NoError(t, err, "Get Object failed")
	got, err := io.ReadAll(reader)
	require.NoError(t, err, "ReadAll returned error")
	requireEqualBigByteSlice(t, exp, got)
}

// compare two big bytearrays one slice at a time(so that we don't blow up the console on error)
func requireEqualBigByteSlice(t *testing.T, exp, actual []byte) {
	t.Helper()
	require.Equal(t, len(exp), len(actual))

	const sliceLen = 100
	sliceCount := len(exp) / sliceLen
	if len(exp)%sliceLen > 0 {
		sliceCount++
	}

	for i := 0; i < sliceCount; i++ {
		var start = i * sliceLen
		var end = min((i+1)*sliceLen, len(exp)-1)

		var expSlice = exp[start:end]
		var actualSlice = actual[start:end]
		require.Equalf(t, expSlice, actualSlice, "Failed on slice "+strconv.Itoa(i+1)+"/"+strconv.Itoa(sliceCount))
	}
}

func objPointers(storageNamespace string) (block.ObjectPointer, block.ObjectPointer) {
	var obj = block.ObjectPointer{
		StorageNamespace: storageNamespace,
		Identifier:       "abc",
		IdentifierType:   block.IdentifierTypeRelative,
	}

	var objCopy = block.ObjectPointer{
		StorageNamespace: storageNamespace,
		Identifier:       "abcCopy",
		IdentifierType:   block.IdentifierTypeRelative,
	}
	return obj, objCopy
}
