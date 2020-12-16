package pyramid

import (
	"os"
)

// ROFile is pyramid wrapper for os.file that implements io.ReadCloser
// with hooks for updating evictions.
type ROFile struct {
	*os.File
	eviction eviction

	rPath relativePath
}

func (f *ROFile) Write(p []byte) (n int, err error) {
	panic("should never write to a read-only file")
}

func (f *ROFile) Sync() error {
	panic("should never write to a read-only file")
}

func (f *ROFile) Read(p []byte) (n int, err error) {
	f.eviction.touch(f.rPath)
	return f.File.Read(p)
}

func (f *ROFile) ReadAt(p []byte, off int64) (n int, err error) {
	f.eviction.touch(f.rPath)
	return f.File.ReadAt(p, off)
}

func (f *ROFile) Stat() (os.FileInfo, error) {
	f.eviction.touch(f.rPath)
	return f.File.Stat()
}
