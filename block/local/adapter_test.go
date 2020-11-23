package local_test

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/block/local"
	"github.com/treeverse/lakefs/testutil"
)

func makeAdapter(t *testing.T) (*local.Adapter, func()) {
	t.Helper()
	dir, err := ioutil.TempDir("", "testing-local-adapter-*")
	testutil.MustDo(t, "TempDir", err)
	testutil.MustDo(t, "NewAdapter", os.MkdirAll(dir, 0700))
	a, err := local.NewAdapter(dir)
	testutil.MustDo(t, "NewAdapter", err)

	return a, func() {
		if _, ok := os.LookupEnv("GOTEST_KEEP_LOCAL"); !ok {
			testutil.MustDo(t, "RemoveAll (cleanup)", os.RemoveAll(dir))
		}
	}
}

func makePointer(path string) block.ObjectPointer {
	return block.ObjectPointer{Identifier: path, StorageNamespace: "local://test/"}
}

func TestLocalPutGet(t *testing.T) {
	a, cleanup := makeAdapter(t)
	defer cleanup()

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

func TestLocalMultipartUpload(t *testing.T) {
	a, cleanup := makeAdapter(t)
	defer cleanup()

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
	a, cleanup := makeAdapter(t)
	defer cleanup()

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
