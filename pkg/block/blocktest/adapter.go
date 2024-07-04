package blocktest

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/ingest/store"
)

// AdapterTest Test suite of basic adapter functionality
func AdapterTest(t *testing.T, adapter block.Adapter, storageNamespace, externalPath string) {
	AdapterBasicObjectTest(t, adapter, storageNamespace, externalPath)
	AdapterMultipartTest(t, adapter, storageNamespace, externalPath)
	t.Run("Adapter_GetRange", func(t *testing.T) { testAdapterGetRange(t, adapter, storageNamespace) })
	t.Run("Adapter_Walker", func(t *testing.T) { testAdapterWalker(t, adapter, storageNamespace) })
	t.Run("Adapter_GetPreSignedURL", func(t *testing.T) { testGetPreSignedURL(t, adapter, storageNamespace) })
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

// Test request for a presigned URL for temporary access
func testGetPreSignedURL(t *testing.T, adapter block.Adapter, storageNamespace string) {
	ctx := context.Background()
	obj, _ := objPointers(storageNamespace)

	preSignedURL, exp, err := adapter.GetPreSignedURL(ctx, obj, block.PreSignModeRead)

	if adapter.BlockstoreType() == block.BlockstoreTypeGS {
		require.ErrorContains(t, err, "no credentials found")
		return
	} else if adapter.BlockstoreType() == block.BlockstoreTypeLocal {
		require.ErrorIs(t, err, block.ErrOperationNotSupported)
		return
	}
	require.NoError(t, err)
	expectedExpiry := expectedURLExp(adapter)
	require.Equal(t, expectedExpiry, exp)
	_, err = url.Parse(preSignedURL)
	require.NoError(t, err)
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

	uri, err := url.Parse(qk.Format())
	require.NoError(t, err, "URL Parse Error")

	w, err := adapter.GetWalker(uri)
	require.NoError(t, err, "GetWalker failed")

	walker := store.NewWrapper(w, uri)

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

func NowMockDefault() time.Time {
	return time.Date(2024, time.January, 0, 0, 0, 0, 0, time.UTC)
}
