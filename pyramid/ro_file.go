package pyramid

import (
	"github.com/treeverse/lakefs/pyramid/params"
	"os"
)

// ROFile is pyramid wrapper for os.File that implements io.ReadCloser
// with hooks for updating evictions.
type ROFile struct {
	*os.File
	eviction params.Eviction

	rPath params.RelativePath
}

func (f *ROFile) Write(p []byte) (n int, err error) {
	panic("should never write to a read-only file")
}

func (f *ROFile) Sync() error {
	panic("should never write to a read-only file")
}

func (f *ROFile) Read(p []byte) (n int, err error) {
	f.eviction.Touch(f.rPath)
	return f.File.Read(p)
}

func (f *ROFile) ReadAt(p []byte, off int64) (n int, err error) {
	f.eviction.Touch(f.rPath)
	return f.File.ReadAt(p, off)
}

func (f *ROFile) Stat() (os.FileInfo, error) {
	f.eviction.Touch(f.rPath)
	return f.File.Stat()
}
