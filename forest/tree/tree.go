package tree

import (
	// cache "github.com/treeverse/lakefs/forest/cache_map"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed/sstable"
)

type treePart struct {
	PartName sstable.ID   `json:"part_name"`
	MaxKey   graveler.Key `json:"max_key"`
}

type Tree struct {
	treeSlice []treePart
}

type DummyMap map[string]string // place holder for cache_map package that will be merged soon

type TreeRepo interface {
	// GetTree returns a tree object. Not sure it is needed because most APIs now return iterators that
	// use a tree internally
	GetTree(treeID graveler.TreeID) (Tree, error)
	GetValue(treeID graveler.TreeID, key graveler.Key) (graveler.ValueRecord, error)
	// NewTreeWriter returns a writer that uses the part manager to create a new tree
	// splitFactor: average number of keys that we want to stored in a part
	// for more detail, look at "IsSplitKey"
	// closeAsync: component used to close part asynchronously, and wait for all part
	//  completions when tree writing completes
	NewTreeWriter(splitFactor int, closeAsync sstable.BatchWriterCloser) TreeWriter
	// experimental interface: a writer that copies a base tree where possible. see below TreeWriterOnBaseTree interface
	NewTreeWriterOnBaseTree(splitFactor int, closeAsync sstable.BatchWriterCloser, treeID graveler.TreeID) TreeWriterOnBaseTree
	// NewIteratorFromTreeID accepts a tree ID, and returns an iterator over the tree
	NewIteratorFromTreeID(treeID graveler.TreeID, start graveler.Key) (graveler.ValueIterator, error)
	// NewIteratorFromTreeObject accept a tree in memory, returns iterator over the tree.
	// If we manage to hide the tree object from tree users completely - this function will become redundant
	NewIteratorFromTreeObject(tree Tree, from graveler.Key) (graveler.ValueIterator, error)
	// GetIterForPart accepts a tree ID and a reading start point. it returns am iterator
	// positioned at the start point. When Next() will be called, first value that is greater-equal
	// than the start key will be returned
	GetIterForPart(sstable.ID, graveler.Key) (graveler.ValueIterator, error)
	// GetIteratorsForDiff accepts the left and right trees of the diff, and finds the common parts which
	// exist in both trees.
	// it returns the left and right value iterators with common parts filtered.
	GetIteratorsForDiff(LeftTree, RightTree graveler.TreeID) (graveler.ValueIterator, graveler.ValueIterator)
}

type TreeWriter interface {
	// WriteValue adds a value to the tree. The value key must be greater than any other key that was written
	// (in other words - values must be entered in sorted by key order)
	// if last insertion operation was AddParts - record key must be greater than any key in the added parts
	WriteValue(record graveler.ValueRecord) error
	// AddParts adds complete parts to the tree at the current insertion point.
	// the added parts can not contain keys smaller than last written value
	AddParts(parts Tree) error
	// FlushIterToTree writes the content of an iterator to the tree.
	FlushIterToTree(iter graveler.ValueIterator) error
	// SaveTree stores the tree to tierFS. During tree writing, parts are closed asynchronously and copied by tierFS
	// while writing continues. SaveTree waits until closing and copying all parts
	SaveTree() (graveler.TreeID, error)
	// SaveTreeWithReusedParts(reuseTree Tree, // A tree may be saved with additional parts that are "reused" from a base tree.
	// these are parts that exist in a source tree, and are merged into the destination tree.
	// an example of using it is in the apply process, which creates a new tree from a base tree and an input iterator.
	// those parts of the base tree that were not modified by it input iterator will be merged into the resulting tree
	// by passing them in the  reuseTree parameter.
	// ) (graveler.TreeID, error)
}

// interface that "inherits" from simple TreeWriter. copies the parts that were not changed from base
type TreeWriterOnBaseTree interface {
	TreeWriter
	// WriteValue overrides the TreeWriter method
	// when writing "jumps" over one or more parts, those parts are are copied to the result tree.
	// handles closing of parts if needed
	WriteValue(record graveler.ValueRecord) error
}
