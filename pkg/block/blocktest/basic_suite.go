package blocktest

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
)

// AdapterBasicObjectTest Test suite of adapter basic functionality on objects
func AdapterBasicObjectTest(t *testing.T, adapter block.Adapter, storageNamespace, externalPath string) {
	t.Run("Adapter_PutGet", func(t *testing.T) { testAdapterPutGet(t, adapter, storageNamespace, externalPath) })
	t.Run("Adapter_Copy", func(t *testing.T) { testAdapterCopy(t, adapter, storageNamespace) })
	t.Run("Adapter_Exists", func(t *testing.T) { testAdapterExists(t, adapter, storageNamespace) })
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
		{"identifier_relative_escaped", block.IdentifierTypeRelative, "special%3Atest_file"},
		{"identifier_full_escaped", block.IdentifierTypeFull, externalPath + "/" + "special%3Atest_file"},
		{"identifier_unknown_relative", block.IdentifierTypeUnknownDeprecated, "test_file"},                  //nolint:staticcheck
		{"identifier_unknown_full", block.IdentifierTypeUnknownDeprecated, externalPath + "/" + "test_file"}, //nolint:staticcheck
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := block.ObjectPointer{
				StorageID:        "",
				StorageNamespace: storageNamespace,
				Identifier:       c.path,
				IdentifierType:   c.identifierType,
			}

			_, err := adapter.Put(ctx, obj, size, strings.NewReader(contents), block.PutOpts{})
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
		StorageID:        "",
		StorageNamespace: storageNamespace,
		Identifier:       "src",
		IdentifierType:   block.IdentifierTypeRelative,
	}
	dst := block.ObjectPointer{
		StorageID:        "",
		StorageNamespace: storageNamespace,
		Identifier:       "export/to/dst",
		IdentifierType:   block.IdentifierTypeRelative,
	}
	_, err := adapter.Put(ctx, src, int64(len(contents)), strings.NewReader(contents), block.PutOpts{})
	require.NoError(t, err)

	require.NoError(t, adapter.Copy(ctx, src, dst))
	reader, err := adapter.Get(ctx, dst)
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, contents, string(got))
}

// Parameterized test of the object Exists method of the Storage adapter
func testAdapterExists(t *testing.T, adapter block.Adapter, storageNamespace string) {
	// TODO (niro): Test abs paths
	const contents = "exists"
	ctx := context.Background()
	_, err := adapter.Put(ctx, block.ObjectPointer{
		StorageID:        "",
		StorageNamespace: storageNamespace,
		Identifier:       contents,
		IdentifierType:   block.IdentifierTypeRelative,
	}, int64(len(contents)), strings.NewReader(contents), block.PutOpts{})
	require.NoError(t, err)

	_, err = adapter.Put(ctx, block.ObjectPointer{
		StorageID:        "",
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
				StorageID:        "",
				StorageNamespace: storageNamespace,
				Identifier:       tt.path,
				IdentifierType:   block.IdentifierTypeRelative,
			})
			require.NoError(t, err)
			require.Equal(t, tt.exists, ok)
		})
	}
}
