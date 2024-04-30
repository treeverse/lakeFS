package fileutil_test

import (
	"errors"
	"io/fs"
	"math/rand"
	"path/filepath"
	"slices"
	"sort"
	"testing"
	"testing/fstest"

	"github.com/go-test/deep"
	"github.com/thanhpk/randstr"

	"github.com/treeverse/lakefs/pkg/fileutil"
)

var basicFiles = []string{
	"z/a/b",
	"z/a/b1",
	"z/a/b2",
	"z/a/c",
	"z/a1",
	"z/a2/b",
	"z/a2/bb/1",
	"z/a2/bb/12/a",
	"z/a2/bb/12/b",
	"z/a2/bb/2",
	"z/a2/bbb",
	"z/a3",
	"z/a31/a/b1",
	"z/a31/a/b2",
	"z/a31/a1/b1",
	"z/a31/a1/b2",
	"z/a31/a2/b",
	"z/a31/b",
}

// trailingFiles holds files and directories that get mixed up when listed
// in sorted order.  Note that "!" < "/".
var trailingFiles = []string{
	"z/a/b",
	"z/a/c",
	"z/a!",
	"z/a1",

	"z/b!/1",
	"z/b!/2",
	"z/b",
	"z/b1/1",
	"z/b1/2",

	"z/c!/1",
	"z/c!/2",
	"z/c/1",
	"z/c/2",
	"z/c1",

	"z/d!",
	"z/d/1",
	"z/d/2",
	"z/d1/1",
	"z/d2/2",
}

func TestWalkSortedFiles(t *testing.T) {
	cases := []struct {
		Name  string
		Files []string
	}{
		{"basic", basicFiles},
		{"trailing", trailingFiles},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			files := slices.Clone(tc.Files)

			fakeFS := make(fstest.MapFS, len(files))
			for _, file := range files {
				fakeFS[file] = &fstest.MapFile{}
			}

			actual := make([]string, 0, len(files))

			err := fileutil.WalkSortedFiles(fakeFS, "z", 3, func(name string, _ fs.DirEntry) {
				actual = append(actual, name)
			})
			if err != nil {
				t.Errorf("WalkSortedFiles: %s", err)
			}

			sort.Sort(sort.StringSlice(files))

			if diffs := deep.Equal(files, actual); diffs != nil {
				t.Errorf("Missing files or files in wrong order: %s", diffs)
				t.Logf("actual: %v", actual)
				t.Logf("expected: %v", files)
			}
		})
	}
}

// addLeaf tries to add a file with path join(parts) as a leaf in fakeFS and
// returns true, or returns false if that path is already a directory there.
// Although MapFS will fake them, it creates intermediate directories so
// that it can operate correctly.
func addLeaf(fakeFS fstest.MapFS, parts []string) bool {
	path := filepath.Join(parts...)
	if _, ok := fakeFS[path]; ok {
		return false
	}
	fakeFS[path] = &fstest.MapFile{}
	for i := len(parts) - 1; i > 0; i-- {
		path = filepath.Join(parts[:i]...)
		if _, ok := fakeFS[path]; ok {
			break
		}
		fakeFS[path] = &fstest.MapFile{Mode: fs.ModeDir}
	}
	return true
}

func exists(fakeFS fstest.MapFS, parts []string) bool {
	path := filepath.Join(parts...)
	_, err := fakeFS.Stat(path)
	switch {
	case err == nil:
		return true
	case errors.Is(err, fs.ErrNotExist):
		return false
	default:
		panic("Unexpected fake FS error " + err.Error())
	}
}

func makeNestedFiles(r *rand.Rand, density int32) fstest.MapFS {
	if density < 1 {
		density = 1
	}
	if density > 80 {
		density = 80
	}

	const maxSize = 140

	size := r.Int()%(maxSize/2) + r.Int()%((maxSize+1)/2)
	fakeFS := make(fstest.MapFS, size)

	parts := []string{"z"}
	for len(fakeFS) < size {
		choice := int32(r.Intn(100))
		switch {
		case len(parts) > 1 && choice < 20:
			// pop directory
			parts = parts[:len(parts)-1]
		case choice < density+20:
			// Add a file
			name := randstr.String(r.Intn(10)+1, randstr.Base62Chars)
			parts = append(parts, name)
			_ = addLeaf(fakeFS, parts)
			parts = parts[:len(parts)-1]
		default:
			for {
				// Add a subdirectory
				name := randstr.String(r.Intn(10)+1, randstr.Base62Chars)
				parts = append(parts, name)
				if !exists(fakeFS, parts) {
					break
				}
				parts = parts[:len(parts)-1]
			}
		}
	}
	return fakeFS
}

func verifyWalker(t testing.TB, fakeFS fstest.MapFS) {
	t.Helper()

	actual := make([]string, 0, len(fakeFS))

	err := fileutil.WalkSortedFiles(fakeFS, "z", 3, func(name string, _ fs.DirEntry) {
		actual = append(actual, name)
	})
	if errors.Is(err, fs.ErrNotExist) {
		if _, ok := fakeFS["z"]; !ok {
			// Fuzzer deleted root directory, not
			// finding anything is correct behaviour!
			return
		}
	}
	if err != nil {
		t.Errorf("WalkSortedFiles: %s", err)
	}

	expected := make([]string, 0, len(fakeFS))
	for f := range fakeFS {
		stat, err := fakeFS.Stat(f)
		if err != nil {
			t.Fatalf("Setup failed: %s: %s", f, err)
		}
		if !stat.IsDir() {
			expected = append(expected, f)
		}
	}
	sort.Sort(sort.StringSlice(expected))

	if diffs := deep.Equal(actual, expected); diffs != nil {
		t.Errorf("Missing files or files in wrong order: %s", diffs)
		t.Logf("actual: %v", actual)
		t.Logf("expected: %v", expected)
	}
}

var seeds = []int32{
	70, // Many files at each level, flat
	50, // More balanced
	20, // Deep
}

func FuzzWalkNestedSortedFiles(f *testing.F) {
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, seed int32) {
		r := rand.New(rand.NewSource(int64(seed)))
		fakeFS := makeNestedFiles(r, seed)
		verifyWalker(t, fakeFS)
	})
}
