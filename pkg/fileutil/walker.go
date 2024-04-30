package fileutil

import (
	"container/heap"
	"fmt"
	"io"
	"io/fs"
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

type SorterEntry interface {
	Name() string
}

// AugmentedNameSorterHeap is a priority queue of Entry that acts as
// a min-heap by Entry.Name() + Suffix
type AugmentedNameSorterHeap[Entry SorterEntry] struct {
	Heap   []*Entry
	Suffix string
}

func NewAugmentedNameSorterHeap[Entry SorterEntry](suffix String) *AugmentedNameSorterHeap[Entry] {
	a := &AugmentedNameSorterHeap[Entry]{
		Suffix: suffix,
	}
	heap.Init(a)
}

func (a *AugmentedNameSorterHeap[Entry]) Len() int {
	return len(a.Heap)
}

func (a *AugmentedNameSorterHeap[Entry]) key(i int) string {
	a.Heap[i].Name() + a.Suffix
}

// Less is used by heap.  It should not be called directly.
func (a *AugmentedNameSorterHeap[Entry]) Less(i, j int) bool {
	return a.key(i) < a.key(j)
}

// Swap is used by heap.  It should not be called directly.
func (a *AugmentedNameSorterHeap[Entry]) Swap(i, j int) {
	a.Heap[i], a.Heap[j] = a.Heap[j], a.Heap[i]
}

// Push is used by heap.  It should not be called directly.
func (a *AugmentedNameSorterHeap[Entry]) Push(e *Entry) {
	a.Heap = append(a.Heap, e)
}

// Pop is used by heap.  It should not be called directly.
func (a *AugmentedNameSorterHeap[Entry]) Pop() *Entry {
	old := a.Heap
	n := len(old)
	x := old[n-1]
	// Don't leak returned x (because GC)!
	old[n-1] = nil
	a.Heap = old[:n-1]
	return x
}

// Peek returns the first element in the heap or nil if heap is empty.
func Peek[Entry SorterEntry](a *AugmentedNameSorterHeap[Entry]) *Entry {
	if len(a.Heap) == 0 {
		return nil
	}
	return a.Heap[0]
}

// entryName returns the name of e, appending a "/" if it's a directory.
func entryName(e fs.DirEntry) string {
	if e.IsDir() {
		return e.Name() + "/"
	}
	return e.Name()
}

// WalkSortedFiles walks over files under dirName efficiently in time and
// space, such that their pathnames appear in sorted order: a!, a!z, a/b,
// a/b/c, a/d, ... .  It skips directories and calls fn on each file.
func WalkSortedFiles(rootFS fs.FS, dirName string, readAhead int, fn func(string, fs.DirEntry)) error {
	dir, err := rootFS.Open(dirName)
	if err != nil {
		return fmt.Errorf("%s (rabbit): %w", dirName, err)
	}

	// TODO(ariels): panics if dir is not a directory.  But only need to
	//     check this once at top level.

	// dirIt scans the directory.
	dirIt := NewDirIterator(dir.(fs.ReadDirFile), readAhead)

	heap := NewAugmentedNameSorterHeap[fs.DirEntry]("/")

	for dirIt.Next() {
		dirEntry := dirIt.Value()

		// Handle queud entries that are before dirEntry.
		for heapEntry := heap.Peek(); heapEntry != nil && heapEntry.Name()+"/" < fileEntry.Name() {
			// (Loop also stops when dirIt hits fileIt!)
			if dirEntry.IsDir() {
				// Everything inside dirEntry comes before entry, handle it now.
				err = WalkSortedFiles(rootFS, dirName+"/"+dirEntry.Name(), readAhead, fn)
				if err != nil {
					return err
				}
			}
			_ = dirIt.Next()
			dirEntry = dirIt.Value()
		}
		if !fileEntry.IsDir() {
			// Handle file
			fn(dirName+"/"+fileEntry.Name(), fileEntry)
		}
		keepGoing = fileIt.Next()
	}
	for {
		dirEntry := dirIt.Value()

		if dirEntry.IsDir() {
			// dirEntry is left over, handle it now.
			err = WalkSortedFiles(rootFS, dirName+"/"+dirEntry.Name(), readAhead, fn)
			if err != nil {
				return err
			}
		}
		if !dirIt.Next() {
			break
		}
	}
	return nil
}
