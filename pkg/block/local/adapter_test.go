package local_test

import (
	"context"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/local"
	"github.com/treeverse/lakefs/pkg/testutil"
)

const testStorageNamespace = "local://test"

func makeAdapter(t *testing.T) *local.Adapter {
	t.Helper()
	a, err := local.NewAdapter(t.TempDir())
	testutil.MustDo(t, "NewAdapter", err)
	return a
}

func makePointer(path string) block.ObjectPointer {
	return block.ObjectPointer{Identifier: path, StorageNamespace: testStorageNamespace}
}

func TestLocalPutExistsGet(t *testing.T) {
	ctx := context.Background()
	a := makeAdapter(t)

	cases := []struct {
		name string
		path string
	}{
		{"simple", "abc"},
		{"nested", "foo/bar"},
	}

	contents := "def"

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			testutil.MustDo(t, "Put", a.Put(ctx, makePointer(c.path), 0, strings.NewReader(contents), block.PutOpts{}))
			ok, err := a.Exists(ctx, makePointer(c.path))
			testutil.MustDo(t, "Exists", err)
			if !ok {
				t.Errorf("expected to detect existence of %s", c.path)
			}
			reader, err := a.Get(ctx, makePointer(c.path), 0)
			testutil.MustDo(t, "Get", err)
			got, err := io.ReadAll(reader)
			testutil.MustDo(t, "ReadAll", err)
			if string(got) != contents {
				t.Errorf("expected to read \"%s\" as written, got \"%s\"", contents, string(got))
			}
		})
	}
}

func TestLocalNotExists(t *testing.T) {
	a := makeAdapter(t)
	ctx := context.Background()

	cases := []string{"missing", "nested/down", "nested/quite/deeply/and/missing"}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			ok, err := a.Exists(ctx, makePointer(c))
			testutil.MustDo(t, "Exists", err)
			if ok {
				t.Errorf("expected not to find %s", c)
			}
		})
	}
}

func TestLocalMultipartUpload(t *testing.T) {
	a := makeAdapter(t)
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
			pointer := makePointer(c.path)
			resp, err := a.CreateMultiPartUpload(ctx, pointer, nil, block.CreateMultiPartUploadOpts{})
			testutil.MustDo(t, "CreateMultiPartUpload", err)
			parts := make([]block.MultipartPart, len(c.partData))
			for partNumber, content := range c.partData {
				partResp, err := a.UploadPart(ctx, pointer, 0, strings.NewReader(content), resp.UploadID, partNumber)
				testutil.MustDo(t, "UploadPart", err)
				parts[partNumber].PartNumber = partNumber + 1
				parts[partNumber].ETag = partResp.ETag
			}
			_, err = a.CompleteMultiPartUpload(ctx, pointer, resp.UploadID, &block.MultipartUploadCompletion{
				Part: parts,
			})
			testutil.MustDo(t, "CompleteMultiPartUpload", err)
			reader, err := a.Get(ctx, pointer, 0)
			testutil.MustDo(t, "Get", err)
			got, err := io.ReadAll(reader)
			testutil.MustDo(t, "ReadAll", err)
			expected := strings.Join(c.partData, "")
			if string(got) != expected {
				t.Errorf("expected to read \"%s\" as written, got \"%s\"", expected, string(got))
			}
		})
	}
}

func TestLocalCopy(t *testing.T) {
	a := makeAdapter(t)
	ctx := context.Background()

	const contents = "foo bar baz quux"
	testutil.MustDo(t, "Put", a.Put(ctx, makePointer("src"), 0, strings.NewReader(contents), block.PutOpts{}))

	testutil.MustDo(t, "Copy", a.Copy(ctx, makePointer("src"), makePointer("export/to/dst")))
	reader, err := a.Get(ctx, makePointer("export/to/dst"), 0)
	testutil.MustDo(t, "Get", err)
	got, err := io.ReadAll(reader)
	testutil.MustDo(t, "ReadAll", err)
	if string(got) != contents {
		t.Errorf("expected to read \"%s\" as written, got \"%s\"", contents, string(got))
	}
}

func TestAdapter_Remove(t *testing.T) {
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
			name:     "single",
			path:     "README",
			wantErr:  false,
			wantTree: []string{""},
		},

		{
			name:     "under folder",
			path:     "src/tools.go",
			wantErr:  false,
			wantTree: []string{""},
		},
		{
			name:     "under multiple folders",
			path:     "a/b/c/d.txt",
			wantErr:  false,
			wantTree: []string{""},
		},
		{
			name:              "file in the way",
			path:              "a/b/c/d.txt",
			additionalObjects: []string{"a/b/blocker.txt"},
			wantErr:           false,
			wantTree:          []string{"", "/test", "/test/a", "/test/a/b", "/test/a/b/blocker.txt"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup env
			adp := makeAdapter(t)
			envObjects := append(tt.additionalObjects, tt.path)
			for _, o := range envObjects {
				obj := makePointer(o)
				testutil.MustDo(t, "Put", adp.Put(ctx, obj, 0, strings.NewReader(content), block.PutOpts{}))
			}

			// test Remove with remove empty folders
			obj := makePointer(tt.path)
			if err := adp.Remove(ctx, obj); (err != nil) != tt.wantErr {
				t.Errorf("Remove() error = %v, wantErr %v", err, tt.wantErr)
			}

			// list and compare
			var tree []string
			if err := filepath.Walk(adp.Path(), func(p string, info fs.FileInfo, err error) error {
				if err != nil {
					return err
				}
				rel := strings.TrimPrefix(p, adp.Path())
				tree = append(tree, rel)
				return nil
			}); err != nil {
				t.Fatal("Walk failed", err)
			}
			if diff := deep.Equal(tt.wantTree, tree); diff != nil {
				t.Errorf("Remove() tree diff = %s", diff)
			}
		})
	}
}
