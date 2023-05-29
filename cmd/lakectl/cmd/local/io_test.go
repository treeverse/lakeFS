package local_test

import (
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/local"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestPruneEmptyDirs(t *testing.T) {
	root, err := os.MkdirTemp("", "lakefs-prune-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if strings.Index(root, "lakefs-prune-test") > 0 {
			_ = os.RemoveAll(root)
		}
	})

	cases := []struct {
		Name     string
		Paths    []string
		Expected []string
	}{
		{
			Name: "prune_deep",
			Paths: []string{
				"a/",
				"a/b/",
				"a/b/c.txt",
				"a/d/",
				"a/e/",
				"a/e/a/",
				"a/e/a/b/",
			},
			Expected: []string{
				"a/d",
				"a/e",
			},
		},
		{
			Name: "prune_deep_keep_neighbor",
			Paths: []string{
				"a/",
				"a/b/",
				"a/b/c.txt",
				"a/d/",
				"a/e/",
				"a/e/a/",
				"a/e/a/b/",
				"b.txt",
				"c/",
				"c/b.txt",
			},
			Expected: []string{
				"a/d",
				"a/e",
			},
		},
		{
			Name:     "prune_keep_root",
			Paths:    []string{},
			Expected: []string{},
		},
		{
			Name: "prune_all",
			Paths: []string{
				"a/",
				"a/b/",
				"a/b/c/",
				"a/d/",
				"a/e/",
				"a/e/a/",
				"a/e/a/b/",
			},
			Expected: []string{
				"a",
			},
		},
	}

	for _, cas := range cases {
		t.Run(cas.Name, func(t *testing.T) {
			currentRoot := filepath.Join(root, cas.Name)
			err = os.Mkdir(currentRoot, 0777)
			if err != nil {
				t.Fatal(err)
			}

			// create directory tree
			for _, entry := range cas.Paths {
				fullPath := filepath.Join(currentRoot, entry)
				if strings.HasSuffix(entry, "/") {
					// create dir
					err = os.Mkdir(fullPath, 0777)
					if err != nil {
						t.Fatal(err)
					}
				} else {
					// create file
					f, err := os.Create(fullPath)
					if err != nil {
						t.Fatal(err)
					}
					err = f.Close()
					if err != nil {
						t.Fatal(err)
					}
				}
			}

			// prune
			removedDirs, err := local.PruneEmptyDirs(currentRoot)
			if err != nil {
				t.Errorf("unexpected error: %s", err.Error())
				return
			}
			// make relative
			removedDirsRel := make([]string, len(removedDirs))
			for i, d := range removedDirs {
				relPath, _ := filepath.Rel(currentRoot, d)
				removedDirsRel[i] = relPath
			}

			// compare
			if !reflect.DeepEqual(removedDirsRel, cas.Expected) {
				t.Errorf("got unexpected removed dirs: %v (expected: %v)", removedDirsRel, cas.Expected)
			}
		})
	}
}

func TestFindInParents(t *testing.T) {
	root, err := os.MkdirTemp("", "lakefs-find-parents-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if strings.Index(root, "lakefs-find-parents-test") > 0 {
			_ = os.RemoveAll(root)
		}
	})

	t.Run("does not exist", func(t *testing.T) {
		found, err := local.FindInParents(root, ".doesnotexist21348329043289")
		if err != nil {
			t.Fatal(err)
		}
		if found != "" {
			t.Errorf("expected found to be empty, got %v", found)
		}
	})

	t.Run("found at root path", func(t *testing.T) {
		deep := filepath.Join(root, "foo1", "bar", "baz")
		fname := filepath.Join(root, "some_file1")
		err := local.CreateDirectoryTree(deep)
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.Create(fname)
		if err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
		found, err := local.FindInParents(deep, "some_file1")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if found != filepath.Join(root, "some_file1") {
			t.Errorf("got path: '%s' expected:'%s'", found, filepath.Join(root, "some_file1"))
		}
	})

	t.Run("found at sub-path", func(t *testing.T) {
		deep := filepath.Join(root, "foo2", "bar", "baz")
		fname := filepath.Join(root, "foo2", "some_file2")
		err := local.CreateDirectoryTree(deep)
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.Create(fname)
		if err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
		found, err := local.FindInParents(deep, "some_file2")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if found != filepath.Join(root, "foo2", "some_file2") {
			t.Errorf("got path: '%s' expected:'%s'", found, filepath.Join(root, "foo2", "some_file2"))
		}
	})

	t.Run("not found above sub-path", func(t *testing.T) {
		deep := filepath.Join(root, "foo3", "bar", "baz")
		fname := filepath.Join(root, "foo3", "some_file3")
		err := local.CreateDirectoryTree(deep)
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.Create(fname)
		if err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
		found, err := local.FindInParents(root, "some_file3")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if found != "" {
			t.Errorf("got path: '%s' expected:''", found)
		}
	})

}
