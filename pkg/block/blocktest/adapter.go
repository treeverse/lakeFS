package blocktest

import (
	"context"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/go-test/deep"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
)

func TestAdapter(t *testing.T, adapter block.Adapter, storageNamespace string) {
	t.Run("Adapter_PutGet", func(t *testing.T) { testAdapterPutGet(t, adapter, storageNamespace) })
	t.Run("Adapter_Copy", func(t *testing.T) { testAdapterCopy(t, adapter, storageNamespace) })
	t.Run("Adapter_Remove", func(t *testing.T) { testAdapterRemove(t, adapter, storageNamespace) })
	t.Run("Adapter_MultipartUpload", func(t *testing.T) { testAdapterMultipartUpload(t, adapter, storageNamespace) })
	t.Run("Adapter_Exists", func(t *testing.T) { testAdapterExists(t, adapter, storageNamespace) })
	t.Run("Adapter_GetRange", func(t *testing.T) { testAdapterGetRange(t, adapter, storageNamespace) })
}

func testAdapterPutGet(t *testing.T, adapter block.Adapter, storageNamespace string) {
	ctx := context.Background()
	contents := "test_file"
	size := int64(len(contents))
	obj := block.ObjectPointer{
		StorageNamespace: storageNamespace,
		Identifier:       contents,
		IdentifierType:   block.IdentifierTypeRelative,
	}
	err := adapter.Put(ctx, obj, size, strings.NewReader(contents), block.PutOpts{})
	require.NoError(t, err)

	reader, err := adapter.Get(ctx, obj, size)
	require.NoError(t, err)
	defer reader.Close()
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, contents, string(got))
}

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

	require.NoError(t, adapter.Put(ctx, src, 0, strings.NewReader(contents), block.PutOpts{}))

	require.NoError(t, adapter.Copy(ctx, src, dst))
	reader, err := adapter.Get(ctx, dst, 0)
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, contents, string(got))
}

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
			envObjects := append(tt.additionalObjects, tt.path)
			for _, p := range envObjects {
				obj := block.ObjectPointer{
					StorageNamespace: storageNamespace,
					Identifier:       tt.name + "/" + p,
					IdentifierType:   block.IdentifierTypeRelative,
				}
				require.NoError(t, adapter.Put(ctx, obj, 0, strings.NewReader(content), block.PutOpts{}))
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
			tree := dumpPathTree(t, ctx, adapter, storageNamespace, tt.name)
			if diff := deep.Equal(tt.wantTree, tree); diff != nil {
				t.Errorf("Remove() tree diff = %s", diff)
			}
		})
	}
}

func dumpPathTree(t testing.TB, ctx context.Context, adapter block.Adapter, storageNamespace, root string) []string {
	t.Helper()
	tree := make([]string, 0)

	err := adapter.Walk(ctx, block.WalkOpts{
		StorageNamespace: storageNamespace,
		Prefix:           root,
	}, func(id string) error {
		_, p, _ := strings.Cut(id, root)
		tree = append(tree, p)
		return nil
	})
	if err != nil {
		t.Fatalf("walking on '%s': %s", root, err)
	}
	sort.Strings(tree)
	return tree
}

func testAdapterMultipartUpload(t *testing.T, adapter block.Adapter, storageNamespace string) {
	ctx := context.Background()

	cases := []struct {
		name     string
		path     string
		partData []string
	}{
		{"simple", "abc", []string{"one ", "two ", "three"}},
		{"nested", "foo/bar", []string{"one ", "two ", "three"}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := block.ObjectPointer{
				StorageNamespace: storageNamespace,
				Identifier:       c.path,
				IdentifierType:   block.IdentifierTypeRelative,
			}
			resp, err := adapter.CreateMultiPartUpload(ctx, obj, nil, block.CreateMultiPartUploadOpts{})
			require.NoError(t, err)
			parts := make([]block.MultipartPart, len(c.partData))
			for partNumber, content := range c.partData {
				partResp, err := adapter.UploadPart(ctx, obj, 0, strings.NewReader(content), resp.UploadID, partNumber)
				require.NoError(t, err)
				parts[partNumber].PartNumber = partNumber + 1
				parts[partNumber].ETag = partResp.ETag
			}
			_, err = adapter.CompleteMultiPartUpload(ctx, obj, resp.UploadID, &block.MultipartUploadCompletion{
				Part: parts,
			})
			require.NoError(t, err)

			reader, err := adapter.Get(ctx, obj, 0)
			require.NoError(t, err)

			got, err := io.ReadAll(reader)
			require.NoError(t, err)

			expected := strings.Join(c.partData, "")
			require.Equal(t, expected, string(got))
		})
	}
}

func testAdapterExists(t *testing.T, adapter block.Adapter, storageNamespace string) {
	ctx := context.Background()
	contents := "exists"
	err := adapter.Put(ctx, block.ObjectPointer{
		StorageNamespace: storageNamespace,
		Identifier:       contents,
		IdentifierType:   block.IdentifierTypeRelative,
	}, 0, strings.NewReader(contents), block.PutOpts{})
	require.NoError(t, err)

	err = adapter.Put(ctx, block.ObjectPointer{
		StorageNamespace: storageNamespace,
		Identifier:       "nested/and/" + contents,
		IdentifierType:   block.IdentifierTypeRelative,
	}, 0, strings.NewReader(contents), block.PutOpts{})
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

func testAdapterGetRange(t *testing.T, adapter block.Adapter, storageNamespace string) {
	ctx := context.Background()
	part1 := "this is the first part "
	part2 := "this is the last part"
	err := adapter.Put(ctx, block.ObjectPointer{
		StorageNamespace: storageNamespace,
		Identifier:       "test_file",
		IdentifierType:   block.IdentifierTypeRelative,
	}, 0, strings.NewReader(part1+part2), block.PutOpts{})
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
		//{"end_smaller_than_start", 10, 1, "", false}, // TODO (niro): To be determinted
		{"negative_position", -1, len(part1), "", true},
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
