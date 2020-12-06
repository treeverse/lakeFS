package pyramid

import (
	"os"
)

// ROFile is pyramid wrapper for os.file that implements io.ReadCloser
// with hooks for updating evictions.
type ROFile struct {
	fh       *os.File
	eviction eviction

	rPath relativePath
}

func (f *ROFile) Read(p []byte) (n int, err error) {
	f.eviction.touch(f.rPath)
	return f.fh.Read(p)
}

func (f *ROFile) ReadAt(p []byte, off int64) (n int, err error) {
	f.eviction.touch(f.rPath)
	return f.fh.ReadAt(p, off)
}

func (f *ROFile) Stat() (os.FileInfo, error) {
	f.eviction.touch(f.rPath)
	return f.fh.Stat()
}

func (f *ROFile) Close() error {
	return f.fh.Close()
}
