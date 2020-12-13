package tree

import (
	// cache "github.com/treeverse/lakefs/forest/cache_map"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed/sstable"
)

type TreePart struct {
	PartName sstable.ID   `json:"part_name"`
	MaxKey   graveler.Key `json:"max_path"`
}
type TreeSlice struct {
	TreeSlice []TreePart
}
type DummyMap map[string]string // place holder for cache_map package that will be merged soon

type TreesRepo struct {
	TreesMap   DummyMap
	PartManger sstable.Manager
}

// InitTreeRepository creates the tree cache, and stores part Manager for operations of parts (currently implemented as sstables).
// should be called at process init.
// decisions on who calls it and how to get a treesRepository will be taken later
type InitTreesRepository func(manager sstable.Manager) TreeRepo

type TreeRepo interface {
	// NewTreeWriter returns a writer that uses the part manager to create a new tree
	NewTreeWriter(splitFactor int, // average number of keys that we want to stored in a part
		// for more detail, look at "IsSplitKey"
		closeAsync sstable.BatchWriterCloser, // component used to close part asynchronously, and wait for all part
		// completions when tree writing completes
	) TreeWriter
	// NewScannerFromTreeID accepts a tree ID, and returns an iterator over the tree
	NewIteratorFromTreeID(treeID graveler.TreeID, start graveler.Key) (graveler.ValueIterator, error)
	// NewIteratorFromTreeParts accept a tree in memory, returns iterator over the tree
	NewIteratorFromTreeSlice(treeSlice TreeSlice, start graveler.Key) (graveler.ValueIterator, error)
	// GetPartManager give components of tree package to the configured part manager.
	// will probably change later
	GetPartManger() sstable.Manager
}

type TreeWriter interface {
	WriteEntry(record graveler.ValueRecord) error
	// FlushIterToTree writes the content of an iterator to the tree.
	FlushIterToTree(iter graveler.ValueIterator) error
	// SaveTree stores the tree to tierFS. During tree writing, parts are closed asynchronously and copied by tierFS
	// while writing continues. SaveTree waits until closing and copying all parts
	SaveTree(reuseTree TreeSlice, // A tree may be saved with additional parts that are "reused" from a base tree.
	// these are parts that exist in a source tree, and are merged into the destination tree.
	// an example of using it is in the apply process, which creates a new tree from a base tree and an input iterator.
	// those parts of the base tree that were not modified by it input iterator will be merged into the resulting tree
	// by passing them in the  reuseTree parameter.
	) (graveler.TreeID, error)
}
