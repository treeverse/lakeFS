package fileutil

import (
	"fmt"
	"io"
	"io/fs"

	"github.com/edwingeng/deque/v2"
)

// DirIterator iterates over an entire directory efficiently in time
// and in space.  It uses the fs.ReadDirFile interface to avoid having to
// hold all directory entries in memory.
type DirIterator struct {
	Dir fs.ReadDirFile
	// ReadAhead is the number of directory entries to try to read at a
	// time.  It must be >= 0.
	ReadAhead int
	// err is the current error, or nil.  Once set, the iterator cannot
	// succeed again.  It is equal to io.EOF at the end of iteration.
	err error
	// entries is the current slice of loaded directory entries.
	entries []fs.DirEntry
	// index is the current index into entries.
	index int
}

func NewDirIterator(dir fs.ReadDirFile, readAhead int) *DirIterator {
	return &DirIterator{
		Dir:       dir,
		ReadAhead: readAhead,
	}
}

func (d *DirIterator) Next() bool {
	d.index++
	if d.index >= len(d.entries) {
		d.entries, d.err = d.Dir.ReadDir(d.ReadAhead)
		if d.err != nil {
			return false
		}
		d.index = 0
	}
	return true
}

func (d *DirIterator) Err() error {
	if d.err == io.EOF {
		return nil
	}
	return d.err
}

func (d *DirIterator) Value() fs.DirEntry {
	return d.entries[d.index]
}

// WalkSortedFiles walks over files under dirName efficiently in time and
// space, such that their pathnames appear in sorted order: a!, a!z, a/b,
// a/b/c, a/d, ... .  It skips directories and calls fn on each file.
func WalkSortedFiles(rootFS fs.FS, dirName string, readAhead int, fn func(string, fs.DirEntry)) error {
	dir, err := rootFS.Open(dirName)
	if err != nil {
		return fmt.Errorf("%s: %w", dirName, err)
	}

	// TODO(ariels): panics if dir is not a directory.  But only need to
	//     check this once at top level.
	it := NewDirIterator(dir.(fs.ReadDirFile), readAhead)

	// queue holds directories waiting to be traversed.  Note that '!',
	// '#' < '/' < '0' < '1' < '9'.  When we encounter a directory $D we
	// cannot traverse it until we see a filename > $D+"/".
	queue := deque.NewDeque[fs.DirEntry]()

	for it.Next() {
		entry := it.Value()
		// Handle  entries that are before entry.
		for {
			nextEntry, ok := queue.TryPopFront()
			if !ok {
				break
			}
			if nextEntry.Name()+"/" > entry.Name() {
				break
			}
			// nextEntry is before entry, handle it now.
			err = WalkSortedFiles(rootFS, dirName+"/"+nextEntry.Name(), readAhead, fn)
			if err != nil {
				return err
			}
		}
		if entry.IsDir() {
			queue.PushBack(entry)
		} else {
			fn(dirName+"/"+entry.Name(), entry)
			continue
		}
	}
	for {
		nextEntry, ok := queue.TryPopFront()
		if !ok {
			break
		}
		// nextEntry is left over, handle it now.
		err = WalkSortedFiles(rootFS, dirName+"/"+nextEntry.Name(), readAhead, fn)
		if err != nil {
			return err
		}
	}
	return nil
}
