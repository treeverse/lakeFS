package blocktest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/go-test/deep"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/ingest/store"
)

const (
	multipartNumberOfParts = 3
	multipartPartSize      = 5 * 1024 * 1024 //generally the minimum size for multi-part upload
)

// AdapterTest Test suite of basic adapter functionality
func AdapterTest(t *testing.T, adapter block.Adapter, storageNamespace, externalPath string) {
	t.Run("Adapter_PutGet", func(t *testing.T) { testAdapterPutGet(t, adapter, storageNamespace, externalPath) })
	t.Run("Adapter_Copy", func(t *testing.T) { testAdapterCopy(t, adapter, storageNamespace) })
	t.Run("Adapter_Remove", func(t *testing.T) { testAdapterRemove(t, adapter, storageNamespace) })
	t.Run("Adapter_MultipartUpload", func(t *testing.T) { testAdapterMultipartUpload(t, adapter, storageNamespace) })
	t.Run("Adapter_AbortMultiPartUpload", func(t *testing.T) { testAdapterAbortMultipartUpload(t, adapter, storageNamespace) })
	t.Run("Adapter_CopyPart", func(t *testing.T) { testAdapterCopyPart(t, adapter, storageNamespace) })
	t.Run("Adapter_CopyPartRange", func(t *testing.T) { testAdapterCopyPartRange(t, adapter, storageNamespace) })
	t.Run("Adapter_Exists", func(t *testing.T) { testAdapterExists(t, adapter, storageNamespace) })
	t.Run("Adapter_GetRange", func(t *testing.T) { testAdapterGetRange(t, adapter, storageNamespace) })
	t.Run("Adapter_Walker", func(t *testing.T) { testAdapterWalker(t, adapter, storageNamespace) })
}

// Parameterized test to first Put object via Storage Adapter then Get it and check that the contents match
func testAdapterPutGet(t *testing.T, adapter block.Adapter, storageNamespace, externalPath string) {
	ctx := context.Background()
	const contents = "test_file"
	size := int64(len(contents))

	cases := []struct {
		name           string
		identifierType block.IdentifierType
		path           string
	}{
		{"identifier_relative", block.IdentifierTypeRelative, "test_file"},
		{"identifier_full", block.IdentifierTypeFull, externalPath + "/" + "test_file"},
		{"identifier_unknown_relative", block.IdentifierTypeUnknownDeprecated, "test_file"},                  //nolint:staticcheck
		{"identifier_unknown_full", block.IdentifierTypeUnknownDeprecated, externalPath + "/" + "test_file"}, //nolint:staticcheck
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := block.ObjectPointer{
				StorageNamespace: storageNamespace,
				Identifier:       c.path,
				IdentifierType:   c.identifierType,
			}

			err := adapter.Put(ctx, obj, size, strings.NewReader(contents), block.PutOpts{})
			require.NoError(t, err)

			reader, err := adapter.Get(ctx, obj)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, reader.Close())
			}()
			got, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, contents, string(got))
		})
	}
}

// Test to Copy an object via Storage Adapter, then check that the contents of the copied object matches the original
func testAdapterCopy(t *testing.T, adapter block.Adapter, storageNamespace string) {
	ctx := context.Background()
	contents := "foo bar baz quux"
	src := block.ObjectPointer{
		StorageNamespace: storageNamespace,
		Identifier:       "src",
		IdentifierType:   block.IdentifierTypeRelative,
	}
	dst := block.ObjectPointer{
		StorageNamespace: storageNamespace,
		Identifier:       "export/to/dst",
		IdentifierType:   block.IdentifierTypeRelative,
	}

	require.NoError(t, adapter.Put(ctx, src, int64(len(contents)), strings.NewReader(contents), block.PutOpts{}))

	require.NoError(t, adapter.Copy(ctx, src, dst))
	reader, err := adapter.Get(ctx, dst)
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, contents, string(got))
}

// Parameterized test to test valid and invalid cases for Removing an object via the adaptor
func testAdapterRemove(t *testing.T, adapter block.Adapter, storageNamespace string) {
	ctx := context.Background()
	const content = "Content used for testing"
	tests := []struct {
		name              string
		additionalObjects []string
		path              string
		wantErr           bool
		wantTree          []string
	}{
		{
			name:     "test_single",
			path:     "README",
			wantErr:  false,
			wantTree: []string{},
		},

		{
			name:     "test_under_folder",
			path:     "src/tools.go",
			wantErr:  false,
			wantTree: []string{},
		},
		{
			name:     "test_under_multiple_folders",
			path:     "a/b/c/d.txt",
			wantErr:  false,
			wantTree: []string{},
		},
		{
			name:              "file_in_the_way",
			path:              "a/b/c/d.txt",
			additionalObjects: []string{"a/b/blocker.txt"},
			wantErr:           false,
			wantTree:          []string{"/a/b/blocker.txt"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup env
			envObjects := tt.additionalObjects
			envObjects = append(envObjects, tt.path)
			for _, p := range envObjects {
				obj := block.ObjectPointer{
					StorageNamespace: storageNamespace,
					Identifier:       tt.name + "/" + p,
					IdentifierType:   block.IdentifierTypeRelative,
				}
				require.NoError(t, adapter.Put(ctx, obj, int64(len(content)), strings.NewReader(content), block.PutOpts{}))
			}

			// test Remove
			obj := block.ObjectPointer{
				StorageNamespace: storageNamespace,
				Identifier:       tt.name + "/" + tt.path,
				IdentifierType:   block.IdentifierTypeRelative,
			}
			if err := adapter.Remove(ctx, obj); (err != nil) != tt.wantErr {
				t.Errorf("Remove() error = %v, wantErr %v", err, tt.wantErr)
			}

			qk, err := adapter.ResolveNamespace(storageNamespace, tt.name, block.IdentifierTypeRelative)
			require.NoError(t, err)

			tree := dumpPathTree(t, ctx, adapter, qk)
			if diff := deep.Equal(tt.wantTree, tree); diff != nil {
				t.Errorf("Remove() tree diff = %s", diff)
			}
		})
	}
}

// Parameterized test of the Multipart Upload APIs. After successful upload we Get the result and compare to the original
func testAdapterMultipartUpload(t *testing.T, adapter block.Adapter, storageNamespace string) {
	ctx := context.Background()
	parts, full := createMultipartContents()

	cases := []struct {
		name string
		path string
	}{
		{"simple", "abc"},
		{"nested", "foo/bar"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			blockstoreType := adapter.BlockstoreType()
			obj := block.ObjectPointer{
				StorageNamespace: storageNamespace,
				Identifier:       c.path,
				IdentifierType:   block.IdentifierTypeRelative,
			}

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
	parts, _ := createMultipartContents()
	obj, _ := objPointers(storageNamespace)
	uploadId := "foobar"

	err := adapter.AbortMultiPartUpload(ctx, obj, uploadId)
	require.ErrorContains(t, err, "404")

	resp, err := adapter.CreateMultiPartUpload(ctx, obj, nil, block.CreateMultiPartUploadOpts{})
	require.NoError(t, err)
	uploadId = resp.UploadID

	multiParts := uploadParts(t, ctx, adapter, obj, uploadId, parts)

	listResp, err := adapter.ListParts(ctx, obj, uploadId, block.ListPartsOpts{})
	require.Equal(t, 3, len(listResp.Parts))

	err = adapter.AbortMultiPartUpload(ctx, obj, uploadId)
	require.NoError(t, err)

	//verify no parts to list
	verifyListInvalid(t, ctx, adapter, obj, uploadId)

	_, err = adapter.CompleteMultiPartUpload(ctx, obj, resp.UploadID, &block.MultipartUploadCompletion{
		Part: multiParts,
	})
	require.ErrorContains(t, err, "404")
}

// Test of the Multipart Copy API
func testAdapterCopyPart(t *testing.T, adapter block.Adapter, storageNamespace string) {
	ctx := context.Background()
	parts, full := createMultipartContents()
	obj, objCopy := objPointers(storageNamespace)
	uploadMultiPart(t, ctx, adapter, obj, parts)

	//Begin Multipart Copy
	resp, err := adapter.CreateMultiPartUpload(ctx, objCopy, nil, block.CreateMultiPartUploadOpts{})
	require.NoError(t, err)
	multiParts := copyPart(t, ctx, adapter, obj, objCopy, resp.UploadID)
	_, err = adapter.CompleteMultiPartUpload(ctx, objCopy, resp.UploadID, &block.MultipartUploadCompletion{
		Part: multiParts,
	})
	require.NoError(t, err)

	getAndCheckContents(t, ctx, adapter, full, objCopy)
}

// Test of the Multipart CopyRange API
func testAdapterCopyPartRange(t *testing.T, adapter block.Adapter, storageNamespace string) {
	ctx := context.Background()
	parts, full := createMultipartContents()
	obj, objCopy := objPointers(storageNamespace)
	uploadMultiPart(t, ctx, adapter, obj, parts)

	//Begin Multipart Copy
	resp, err := adapter.CreateMultiPartUpload(ctx, objCopy, nil, block.CreateMultiPartUploadOpts{})
	require.NoError(t, err)
	multiParts := copyPartRange(t, ctx, adapter, obj, objCopy, resp.UploadID)
	_, err = adapter.CompleteMultiPartUpload(ctx, objCopy, resp.UploadID, &block.MultipartUploadCompletion{
		Part: multiParts,
	})
	require.NoError(t, err)

	getAndCheckContents(t, ctx, adapter, full, objCopy)
}

// Parameterized test of the object Exists method of the Storage adapter
func testAdapterExists(t *testing.T, adapter block.Adapter, storageNamespace string) {
	// TODO (niro): Test abs paths
	const contents = "exists"
	ctx := context.Background()
	err := adapter.Put(ctx, block.ObjectPointer{
		StorageNamespace: storageNamespace,
		Identifier:       contents,
		IdentifierType:   block.IdentifierTypeRelative,
	}, int64(len(contents)), strings.NewReader(contents), block.PutOpts{})
	require.NoError(t, err)

	err = adapter.Put(ctx, block.ObjectPointer{
		StorageNamespace: storageNamespace,
		Identifier:       "nested/and/" + contents,
		IdentifierType:   block.IdentifierTypeRelative,
	}, int64(len(contents)), strings.NewReader(contents), block.PutOpts{})
	require.NoError(t, err)

	cases := []struct {
		name   string
		path   string
		exists bool
	}{
		{"exists", "exists", true},
		{"nested_exists", "nested/and/exists", true},
		{"simple_missing", "missing", false},
		{"nested_missing", "nested/down", false},
		{"nested_deep_missing", "nested/quite/deeply/and/missing", false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ok, err := adapter.Exists(ctx, block.ObjectPointer{
				StorageNamespace: storageNamespace,
				Identifier:       tt.path,
				IdentifierType:   block.IdentifierTypeRelative,
			})
			require.NoError(t, err)
			require.Equal(t, tt.exists, ok)
		})
	}
}

// Parameterized test of the GetRange functionality
func testAdapterGetRange(t *testing.T, adapter block.Adapter, storageNamespace string) {
	ctx := context.Background()
	part1 := "this is the first part "
	part2 := "this is the last part"
	err := adapter.Put(ctx, block.ObjectPointer{
		StorageNamespace: storageNamespace,
		Identifier:       "test_file",
		IdentifierType:   block.IdentifierTypeRelative,
	}, int64(len(part1+part2)), strings.NewReader(part1+part2), block.PutOpts{})
	require.NoError(t, err)

	cases := []struct {
		name          string
		startPos      int
		endPos        int
		expected      string
		expectFailure bool
	}{
		{"read_suffix", len(part1), len(part1 + part2), part2, false},
		{"read_prefix", 0, len(part1) - 1, part1, false},
		{"read_middle", 8, len(part1) + 6, "the first part this is", false},
		// {"end_smaller_than_start", 10, 1, "", false}, // TODO (niro): To be determined
		// {"negative_position", -1, len(part1), "", true}, // S3 and Azure not aligned
		{"one_byte", 1, 1, string(part1[1]), false},
		{"out_of_bounds", 0, len(part1+part2) + 10, part1 + part2, false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			reader, err := adapter.GetRange(ctx, block.ObjectPointer{
				StorageNamespace: storageNamespace,
				Identifier:       "test_file",
				IdentifierType:   block.IdentifierTypeRelative,
			}, int64(tt.startPos), int64(tt.endPos))
			require.Equal(t, tt.expectFailure, err != nil)
			if err == nil {
				got, err := io.ReadAll(reader)
				require.NoError(t, err)
				require.Equal(t, tt.expected, string(got))
			}
		})
	}
}

// Parameterized test to GetWalker from the Storage Adapter and check that it works
func testAdapterWalker(t *testing.T, adapter block.Adapter, storageNamespace string) {
	ctx := context.Background()
	const (
		testPrefix      = "test_walker"
		filesAndFolders = 5
		contents        = "test_file"
	)

	for i := 0; i < filesAndFolders; i++ {
		for j := 0; j < filesAndFolders; j++ {
			err := adapter.Put(ctx, block.ObjectPointer{
				StorageNamespace: storageNamespace,
				Identifier:       fmt.Sprintf("%s/folder_%d/test_file_%d", testPrefix, filesAndFolders-i-1, filesAndFolders-j-1),
				IdentifierType:   block.IdentifierTypeRelative,
			}, int64(len(contents)), strings.NewReader(contents), block.PutOpts{})
			require.NoError(t, err)
		}
	}

	err := adapter.Put(ctx, block.ObjectPointer{
		StorageNamespace: storageNamespace,
		Identifier:       fmt.Sprintf("%s/folder_0.txt", testPrefix),
		IdentifierType:   block.IdentifierTypeRelative,
	}, int64(len(contents)), strings.NewReader(contents), block.PutOpts{})
	require.NoError(t, err)

	cases := []struct {
		name   string
		prefix string
	}{
		{
			name:   "root",
			prefix: "",
		},
		{
			name:   "prefix",
			prefix: "folder_1",
		},
		{
			name:   "prefix/",
			prefix: "folder_2",
		},
	}
	for _, tt := range cases {
		qk, err := adapter.ResolveNamespace(storageNamespace, filepath.Join(testPrefix, tt.prefix), block.IdentifierTypeRelative)
		require.NoError(t, err)
		uri, err := url.Parse(qk.Format())
		require.NoError(t, err)
		t.Run(tt.name, func(t *testing.T) {
			reader, err := adapter.GetWalker(uri)
			require.NoError(t, err)

			var results []string
			err = reader.Walk(ctx, uri, block.WalkOptions{}, func(e block.ObjectStoreEntry) error {
				results = append(results, e.RelativeKey)
				return nil
			})
			require.NoError(t, err)
			var prefix string
			if tt.prefix == "" {
				if adapter.BlockstoreType() != block.BlockstoreTypeLocal {
					prefix = testPrefix
				}

				require.Equal(t, path.Join(prefix, "folder_0.txt"), results[0])
				results = results[1:]
				for i := 0; i < filesAndFolders; i++ {
					for j := 0; j < filesAndFolders; j++ {
						require.Equal(t, path.Join(prefix, fmt.Sprintf("folder_%d/test_file_%d", i, j)), results[i*filesAndFolders+j])
					}
				}
			} else {
				if adapter.BlockstoreType() != block.BlockstoreTypeLocal {
					prefix = tt.prefix
				}
				for j := 0; j < filesAndFolders; j++ {
					require.Equal(t, path.Join(prefix, fmt.Sprintf("test_file_%d", j)), results[j])
				}
			}
		})
	}
}

func createMultipartContents() ([][]byte, []byte) {

	parts := make([][]byte, multipartNumberOfParts)
	var partsConcat []byte
	for i := 0; i < multipartNumberOfParts; i++ {
		parts[i] = randstr.Bytes(multipartPartSize + i)
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

// List parts on non-existing part
func verifyListInvalid(t *testing.T, ctx context.Context, adapter block.Adapter, obj block.ObjectPointer, uploadId string) {
	t.Helper()
	_, err := adapter.ListParts(ctx, obj, uploadId, block.ListPartsOpts{})
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
		require.NoError(t, err)
		multiParts[i].PartNumber = partNumber
		multiParts[i].ETag = partResp.ETag
		startPosition = endPosition + 1
	}

	return multiParts
}

// Copy one and only part after starting a multipart upload
func copyPart(t *testing.T, ctx context.Context, adapter block.Adapter, obj, objCopy block.ObjectPointer, uploadID string) []block.MultipartPart {
	t.Helper()
	multiParts := make([]block.MultipartPart, 1)
	partNumber := 1
	partResp, err := adapter.UploadCopyPart(ctx, obj, objCopy, uploadID, partNumber)
	require.NoError(t, err)
	multiParts[0].PartNumber = partNumber
	multiParts[0].ETag = partResp.ETag

	return multiParts
}

func getAndCheckContents(t *testing.T, ctx context.Context, adapter block.Adapter, exp []byte, obj block.ObjectPointer) {
	t.Helper()
	//first check exists
	ok, err := adapter.Exists(ctx, obj)
	require.NoError(t, err)
	require.Equal(t, true, ok)

	reader, err := adapter.Get(ctx, obj)
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
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

func dumpPathTree(t testing.TB, ctx context.Context, adapter block.Adapter, qk block.QualifiedKey) []string {
	t.Helper()
	tree := make([]string, 0)

	uri, err := url.Parse(qk.Format())
	require.NoError(t, err)

	w, err := adapter.GetWalker(uri)
	require.NoError(t, err)

	walker := store.NewWrapper(w, uri)
	require.NoError(t, err)

	err = walker.Walk(ctx, block.WalkOptions{}, func(e block.ObjectStoreEntry) error {
		_, p, _ := strings.Cut(e.Address, uri.String())
		tree = append(tree, p)
		return nil
	})
	if err != nil {
		t.Fatalf("walking on '%s': %s", uri.String(), err)
	}
	sort.Strings(tree)
	return tree
}
