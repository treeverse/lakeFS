package local_test

import (
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
	os.MkdirAll(dir, 0700)
	a, err := local.NewAdapter(dir)
	testutil.MustDo(t, "NewAdapter", err)

	return a, func() {
		if _, ok := os.LookupEnv("GOTEST_KEEP_LOCAL"); !ok {
			testutil.MustDo(t, "RemoveAll (cleanup)", os.RemoveAll(dir))
		}
	}
}

func makePointer(path string) block.ObjectPointer {
	return block.ObjectPointer{Identifier: path}
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
