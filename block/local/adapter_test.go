package local_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-test/deep"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/block/local"
	"github.com/treeverse/lakefs/testutil"
)

func makeAdapter(t *testing.T) *local.Adapter {
	t.Helper()
	dir, err := ioutil.TempDir("", "testing-local-adapter-*")
	testutil.MustDo(t, "TempDir", err)
	testutil.MustDo(t, "NewAdapter", os.MkdirAll(dir, 0700))
	a, err := local.NewAdapter(dir)
	testutil.MustDo(t, "NewAdapter", err)

	t.Cleanup(func() {
		if _, ok := os.LookupEnv("GOTEST_KEEP_LOCAL"); ok {
			return
		}
		_ = os.RemoveAll(dir)
	})
	return a
}

func makePointer(path string) block.ObjectPointer {
	return block.ObjectPointer{Identifier: path, StorageNamespace: "local://test/"}
}

func TestLocalPutExistsGet(t *testing.T) {
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
			testutil.MustDo(t, "Put", a.Put(makePointer(c.path), 0, strings.NewReader(contents), block.PutOpts{}))
			ok, err := a.Exists(makePointer(c.path))
			testutil.MustDo(t, "Exists", err)
			if !ok {
				t.Errorf("expected to detect existence of %s", c.path)
			}
			reader, err := a.Get(makePointer(c.path), 0)
			testutil.MustDo(t, "Get", err)
			got, err := ioutil.ReadAll(reader)
			testutil.MustDo(t, "ReadAll", err)
			if string(got) != contents {
				t.Errorf("expected to read \"%s\" as written, got \"%s\"", contents, string(got))
			}
		})
	}
}

func TestLocalNotExists(t *testing.T) {
	a := makeAdapter(t)

	cases := []string{"missing", "nested/down", "nested/quite/deeply/and/missing"}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			ok, err := a.Exists(makePointer(c))
			testutil.MustDo(t, "Exists", err)
			if ok {
				t.Errorf("expected not to find %s", c)
			}
		})
	}
}

func TestLocalMultipartUpload(t *testing.T) {
	a := makeAdapter(t)

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
			uploadID, err := a.CreateMultiPartUpload(pointer, nil, block.CreateMultiPartUploadOpts{})
			testutil.MustDo(t, "CreateMultiPartUpload", err)
			parts := make([]*s3.CompletedPart, 0)
			for partNumber, content := range c.partData {
				cs, err := a.UploadPart(pointer, 0, strings.NewReader(content), uploadID, int64(partNumber))
				testutil.MustDo(t, "UploadPart", err)
				parts = append(parts, &s3.CompletedPart{
					ETag:       aws.String(cs),
					PartNumber: aws.Int64(int64(partNumber)),
				})
			}
			_, _, err = a.CompleteMultiPartUpload(pointer, uploadID, &block.MultipartUploadCompletion{
				Part: parts,
			})
			testutil.MustDo(t, "CompleteMultiPartUpload", err)
			reader, err := a.Get(pointer, 0)
			testutil.MustDo(t, "Get", err)
			got, err := ioutil.ReadAll(reader)
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

	contents := "foo bar baz quux"

	testutil.MustDo(t, "Put", a.Put(makePointer("src"), 0, strings.NewReader(contents), block.PutOpts{}))

	testutil.MustDo(t, "Copy", a.Copy(makePointer("src"), makePointer("export/to/dst")))
	reader, err := a.Get(makePointer("export/to/dst"), 0)
	testutil.MustDo(t, "Get", err)
	got, err := ioutil.ReadAll(reader)
	testutil.MustDo(t, "ReadAll", err)
	if string(got) != contents {
		t.Errorf("expected to read \"%s\" as written, got \"%s\"", contents, string(got))
	}
}

func TestRemove(t *testing.T) {
	a := makeAdapter(t)
	const contents = "foo bar baz quux"

	obj1 := makePointer("README")
	err := a.Put(obj1, 0, strings.NewReader(contents), block.PutOpts{})
	testutil.MustDo(t, "Put", err)
	err = a.Remove(obj1)
	testutil.MustDo(t, "Remove", err)

	root := a.Path()
	tree1 := dumpPathTree(t, root)
	if diff := deep.Equal(tree1, []string{""}); diff != nil {
		t.Fatalf("Found diff after remove: %s", diff)
	}

	obj2 := makePointer("src/tools.go")
	err = a.Put(obj2, 0, strings.NewReader(contents), block.PutOpts{})
	testutil.MustDo(t, "Put", err)
	err = a.Remove(obj2)
	testutil.MustDo(t, "Remove", err)
	tree2 := dumpPathTree(t, root)
	if diff := deep.Equal(tree2, []string{""}); diff != nil {
		t.Fatalf("Found diff after remove: %s", diff)
	}

	obj3 := makePointer("a/b/c/d.txt")
	err = a.Put(obj3, 0, strings.NewReader(contents), block.PutOpts{})
	testutil.MustDo(t, "Put", err)
	err = a.Remove(obj3)
	testutil.MustDo(t, "Remove", err)
	tree3 := dumpPathTree(t, root)
	if diff := deep.Equal(tree3, []string{""}); diff != nil {
		t.Fatalf("Found diff after remove: %s", diff)
	}
}

func dumpPathTree(t testing.TB, root string) []string {
	tree := make([]string, 0)
	err := filepath.Walk(root, func(path string, _ os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		p := strings.TrimPrefix(path, root)
		tree = append(tree, p)
		return nil
	})
	if err != nil {
		t.Fatalf("walking on '%s': %s", root, err)
	}
	return tree
}
