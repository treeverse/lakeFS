package blocktest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/url"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
)

// AdapterTest Test suite of basic adapter functionality
func AdapterTest(t *testing.T, adapter block.Adapter, storageNamespace, externalPath string) {
	AdapterBasicObjectTest(t, adapter, storageNamespace, externalPath)
	AdapterMultipartTest(t, adapter, storageNamespace, externalPath)
	t.Run("Adapter_GetRange", func(t *testing.T) { testAdapterGetRange(t, adapter, storageNamespace) })
	t.Run("Adapter_Walker", func(t *testing.T) { testAdapterWalker(t, adapter, storageNamespace) })
	t.Run("Adapter_GetPreSignedURL", func(t *testing.T) { testGetPreSignedURL(t, adapter, storageNamespace) })
}

func AdapterPresignedEndpointOverrideTest(t *testing.T, adapter block.Adapter, storageNamespace, externalPath string, oe *url.URL) {
	AdapterBasicObjectTest(t, adapter, storageNamespace, externalPath)
	t.Run("Adapter_GetPreSignedURLEndpointOverride", func(t *testing.T) { testGetPreSignedURLEndpointOverride(t, adapter, storageNamespace, oe) })
}

// Parameterized test of the GetRange functionality
func testAdapterGetRange(t *testing.T, adapter block.Adapter, storageNamespace string) {
	ctx := context.Background()
	part1 := "this is the first part "
	part2 := "this is the last part"
	_, err := adapter.Put(ctx, block.ObjectPointer{
		StorageID:        "",
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
				StorageID:        "",
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
			_, err := adapter.Put(ctx, block.ObjectPointer{
				StorageID:        "",
				StorageNamespace: storageNamespace,
				Identifier:       fmt.Sprintf("%s/folder_%d/test_file_%d", testPrefix, filesAndFolders-i-1, filesAndFolders-j-1),
				IdentifierType:   block.IdentifierTypeRelative,
			}, int64(len(contents)), strings.NewReader(contents), block.PutOpts{})
			require.NoError(t, err)
		}
	}

	_, err := adapter.Put(ctx, block.ObjectPointer{
		StorageID:        "",
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
		qk, err := adapter.ResolveNamespace("", storageNamespace, filepath.Join(testPrefix, tt.prefix), block.IdentifierTypeRelative)
		require.NoError(t, err)
		uri, err := url.Parse(qk.Format())
		require.NoError(t, err)
		t.Run(tt.name, func(t *testing.T) {
			reader, err := adapter.GetWalker("", block.WalkerOptions{StorageURI: uri})
			require.NoError(t, err)

			var results []string
			err = reader.Walk(ctx, uri, block.WalkOptions{}, func(e block.ObjectStoreEntry) error {
				results = append(results, e.RelativeKey)
				return nil
			})
			require.NoError(t, err)

			// prepare expectedResults
			var (
				expectedResults []string
				prefix          string
			)
			if tt.prefix == "" {
				if adapter.BlockstoreType() != block.BlockstoreTypeLocal {
					prefix = testPrefix
				}
				expectedResults = append(expectedResults, path.Join(prefix, "folder_0.txt"))
				for i := 0; i < filesAndFolders; i++ {
					for j := 0; j < filesAndFolders; j++ {
						expectedResults = append(expectedResults, path.Join(prefix, fmt.Sprintf("folder_%d/test_file_%d", i, j)))
					}
				}
			} else {
				if adapter.BlockstoreType() != block.BlockstoreTypeLocal {
					prefix = tt.prefix
				}
				for j := 0; j < filesAndFolders; j++ {
					expectedResults = append(expectedResults, path.Join(prefix, fmt.Sprintf("test_file_%d", j)))
				}
			}
			// check that the results match the expected results
			if diff := deep.Equal(expectedResults, results); diff != nil {
				t.Errorf("Mismatch in expected results: %v", diff)
			}
		})
	}
}

// Test request for a presigned URL for temporary access
func testGetPreSignedURL(t *testing.T, adapter block.Adapter, storageNamespace string) {
	// without filename
	preSignedURL, exp := getPresignedURLBasicTest(t, adapter, storageNamespace, "")
	_, err := url.Parse(preSignedURL)
	require.NoError(t, err)
	require.NotNil(t, exp)
	require.Equal(t, expectedURLExp(adapter), *exp)

	// with filename
	const filename = "test_file"
	preSignedURL, exp = getPresignedURLBasicTest(t, adapter, storageNamespace, filename)
	parsedURL, err := url.Parse(preSignedURL)
	require.NoError(t, err)
	require.NotNil(t, exp)
	require.Equal(t, expectedURLExp(adapter), *exp)

	// Parse and verify content-disposition from URL query parameters
	queryParams := parsedURL.Query()

	// extract the content disposition value from the query parameters
	var contentDisposition string
	for _, param := range []string{"response-content-disposition", "rscd"} {
		contentDisposition = queryParams.Get(param)
		if contentDisposition != "" {
			break
		}
	}
	require.NotEmpty(t, contentDisposition, "Content disposition parameter not found in URL")

	// Parse the content-disposition to extract the filename
	_, params, err := mime.ParseMediaType(contentDisposition)
	require.NoError(t, err, "Failed to parse content disposition")

	parsedFilename := params["filename"]
	require.Equal(t, filename, parsedFilename, "Extracted filename doesn't match expected value")
}

// Test request for a presigned URL with an endpoint override
func testGetPreSignedURLEndpointOverride(t *testing.T, adapter block.Adapter, storageNamespace string, oe *url.URL) {
	preSignedURL, exp := getPresignedURLBasicTest(t, adapter, storageNamespace, "")
	require.NotNil(t, exp)
	expectedExpiry := expectedURLExp(adapter)
	require.Equal(t, expectedExpiry, *exp)
	u, err := url.Parse(preSignedURL)
	require.NoError(t, err)
	require.Equal(t, u.Scheme, oe.Scheme)
	require.Contains(t, u.Host, oe.Host)
}

func getPresignedURLBasicTest(t *testing.T, adapter block.Adapter, storageNamespace string, filename string) (string, *time.Time) {
	ctx := context.Background()
	obj, _ := objPointers(storageNamespace)

	preSignedURL, exp, err := adapter.GetPreSignedURL(ctx, obj, block.PreSignModeRead, filename)
	if errors.Is(err, block.ErrOperationNotSupported) {
		t.Skip("GetPreSignedURL not supported")
	}
	// Google storage returns an error if no credentials are found, and we can't sign the URL
	if err != nil && strings.Contains(err.Error(), "no credentials found") {
		t.Skip("GetPreSignedURL no credentials found")
	}
	require.NoError(t, err)
	return preSignedURL, &exp
}

func expectedURLExp(adapter block.Adapter) time.Time {
	if adapter.BlockstoreType() == block.BlockstoreTypeAzure {
		// we didn't implement expiry for Azure yet
		return time.Time{}
	} else {
		return NowMockDefault().Add(block.DefaultPreSignExpiryDuration)
	}
}

func dumpPathTree(t testing.TB, ctx context.Context, adapter block.Adapter, qk block.QualifiedKey) []string {
	t.Helper()
	tree := make([]string, 0)

	p := qk.Format()
	uri, err := url.Parse(p)
	require.NoError(t, err, "URL Parse Error")

	walker, err := adapter.GetWalker("", block.WalkerOptions{StorageURI: uri})
	require.NoError(t, err, "GetWalker failed")

	wwalker := block.NewWalkerWrapper(walker, uri)
	err = wwalker.Walk(ctx, block.WalkOptions{}, func(e block.ObjectStoreEntry) error {
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

func NowMockDefault() time.Time {
	return time.Date(2024, time.January, 0, 0, 0, 0, 0, time.UTC)
}
