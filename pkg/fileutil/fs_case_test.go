package fileutil_test

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/pkg/fileutil"
)

func fatal(t testing.TB) func(string) {
	return func(message string) {
		t.Fatal(message)
	}
}

type nothing struct{}

type PathSet map[string]nothing

// PathMapFS fakes a fileutil.FS that maps all pathnames through a mapping.
type PathMapFS struct {
	PathMapper func(string) string
	Paths      PathSet

	// If non-nil, return this error for any operation on a path that
	// starts with ErrorPathPrefix.
	Err error
	// Prefix to use for reporting Err.
	ErrorPathPrefix string
}

func (f *PathMapFS) Touch(path string) error {
	path = f.PathMapper(path)
	if f.Err != nil && strings.HasPrefix(path, f.ErrorPathPrefix) {
		return f.Err
	}

	if _, ok := f.Paths[path]; ok {
		return os.ErrExist
	}
	f.Paths[path] = nothing{}
	return nil
}

func (f *PathMapFS) Exists(path string) (bool, error) {
	path = f.PathMapper(path)
	if f.Err != nil && strings.HasPrefix(path, f.ErrorPathPrefix) {
		return false, f.Err
	}

	_, ok := f.Paths[path]
	return ok, nil
}

func (f *PathMapFS) Remove(path string) error {
	path = f.PathMapper(path)
	if f.Err != nil && strings.HasPrefix(path, f.ErrorPathPrefix) {
		return f.Err
	}

	if _, ok := f.Paths[path]; ok {
		delete(f.Paths, path)
		return nil
	}
	return os.ErrNotExist
}

// CaseSensitiveFS fakes a case-sensitive fileutil.FS, that returns errors
// for some paths.
func CaseSensitiveFS() *PathMapFS {
	return &PathMapFS{
		PathMapper: func(p string) string { return p },
		Paths:      make(PathSet),
	}
}

// CaseInsensitiveFS fakes a case-sensitive fileutil.FS.
func CaseInsensitiveFS() *PathMapFS {
	return &PathMapFS{
		PathMapper: func(p string) string { return strings.ToLower(p) },
		Paths:      make(PathSet),
	}
}

func TestIsCaseInsensitiveLocationFalse(t *testing.T) {
	fs := CaseSensitiveFS()

	insensitive, err := fileutil.IsCaseInsensitiveLocation(fs, "/home/me/dir", fatal(t))
	if insensitive {
		t.Error("Expected case-sensitive FS to be reported as such")
	}
	if err != nil {
		t.Errorf("Failed to test case-sensitive FS: %s", err)
	}
}

func TestIsCaseInsensitiveLocationTrue(t *testing.T) {
	fs := CaseInsensitiveFS()

	insensitive, err := fileutil.IsCaseInsensitiveLocation(fs, "/home/me/dir", fatal(t))
	if !insensitive {
		t.Error("Expected case-insensitive FS to be reported as such")
	}
	if err != nil {
		t.Errorf("Failed to test case-sensitive FS: %s", err)
	}
}

func TestIsCaseInsensitiveLocationError(t *testing.T) {
	testingErr := errors.New("for testing")

	fs := CaseSensitiveFS()
	fs.Err = testingErr
	fs.ErrorPathPrefix = "/home/me/err/"

	_, err := fileutil.IsCaseInsensitiveLocation(fs, "/home/me/err/", fatal(t))
	if !errors.Is(err, testingErr) {
		t.Errorf("Got error %s when expecting %s", err, testingErr)
	}
}

// TestOSIsCaseInsensitiveLocation tests that IsCaseInsensitiveLocation
// works on the OS.  It cannot test the result, as it does not know what to
// expect.
func TestOSIsCaseInsensitiveLocation(t *testing.T) {
	fs := fileutil.NewOSFS()
	tempDir := t.TempDir()
	isCaseInsensitive, err := fileutil.IsCaseInsensitiveLocation(fs, tempDir, fatal(t))

	if err != nil {
		t.Errorf("IsCaseInsensitiveLocation failed: %s", err)
	}

	if isCaseInsensitive {
		t.Logf("Case-insensitive directory: %s", tempDir)
	} else {
		t.Logf("Case-sensitive directory: %s", tempDir)
	}
}
