package tree

import (
	cache "github.com/treeverse/lakefs/forest/cache_map"
	gr "github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed/sstable"
)

type TreePart struct {
	PartName sstable.ID `json:"part_name"`
	MaxKey   gr.Key     `json:"max_path"`
}
type TreeSlice struct {
	treeSlice []TreePart
}

type treesRepo struct {
	treesMap   cache.CacheMap
	partManger sstable.Manager
}

// InitTreeRepository creates the trees cache, and stores part Manager for operations of parts (currently implemented as sstables).
// should be called at process start.
// decisions on who calls it and how to get a treesRepository will be taken later
type InitTreesRepository func(manager sstable.Manager) TreeRepo

type TreeRepo interface {
	// NewTreeWriter returns a writer that uses the part managet to create a new tree
	NewTreeWriter(splitFactor int, // average number of keys that we want to stored in a part
		// for more detail, look at "IsSplitKey"
		closeAsync sstable.BatchWriterCloser, // component used to close part asynchronously, and wait for all part
		// completions when tree writing completes
	) TreeWriter
	// NewScannerFromTreeID accepts a tree ID, and returnes an iterator over the tree
	NewIteratorFromTreeID(treeID gr.TreeID, start gr.Key) (gr.ValueIterator, error)
	// NewIteratorFromTreeParts accept a tree in memory, returnes iferator over the tree
	NewIteratorFromTreeSlice(treeSlice TreeSlice, start gr.Key) (gr.ValueIterator, error)
	// GetPartManager give components of tree package to the configured part manager.
	// will probably change later
	GetPartManger() sstable.Manager
}

type TreeWriter interface {
	WriteEntry(record gr.ValueRecord) error
	FlushIterToTree(iter gr.ValueIterator) error
	SaveTree(reuseTree TreeSlice) (gr.TreeID, error)
}
