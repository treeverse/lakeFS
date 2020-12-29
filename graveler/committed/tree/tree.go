package tree

//go:generate mockgen -source=tree.go -destination=mock/tree.go -package=mock

import (
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed"
	"google.golang.org/protobuf/proto"
)

// Part is the basic building stone of a tree
type Part struct {
	ID            committed.ID
	MinKey        committed.Key
	MaxKey        committed.Key
	EstimatedSize uint64 // EstimatedSize estimated part size in bytes
}

func MarshalPart(part Part) ([]byte, error) {
	return proto.Marshal(&PartData{
		MinKey:        part.MinKey,
		MaxKey:        part.MaxKey,
		EstimatedSize: part.EstimatedSize,
	})
}

func UnmarshalPart(b []byte) (Part, error) {
	var p PartData
	err := proto.Unmarshal(b, &p)
	if err != nil {
		return Part{}, err
	}
	return Part{
		MinKey:        p.MinKey,
		MaxKey:        p.MaxKey,
		EstimatedSize: p.EstimatedSize,
	}, nil
}

// Tree is a sorted slice of parts with no overlapping between the parts
type Tree struct {
	ID    graveler.TreeID
	Parts []Part
}

// Iterator iterates over all part headers and values of a tree, allowing seeking by entire
// parts.
type Iterator interface {
	// Next moves to look at the next value in the current part, or a header for the next
	// part if the current part is over.
	Next() bool
	// NextPart() skips over the entire remainder of the current part and continues at the
	// header for the next part.
	NextPart() bool
	// Value returns a nil ValueRecord and a Part before starting a part, or a Value and
	// that Part when inside a part.
	Value() (*graveler.ValueRecord, *Part)
	SeekGE(id graveler.Key)
	Err() error
	Close()
}

type RepoProvider interface {
	GetRepo(ns graveler.StorageNamespace) Repo
}

// Repo is an abstraction for a repository of trees that exposes operations on them
type Repo interface {
	GetTree(treeID graveler.TreeID) (*Tree, error)

	// GetValue finds the matching graveler.ValueRecord in the tree with the treeID
	GetValue(treeID graveler.TreeID, key graveler.Key) (*graveler.ValueRecord, error)

	// NewTreeWriter returns a writer that is used for creating new trees
	NewTreeWriter() Writer

	// NewIterator accepts a tree ID, and returns an iterator
	// over the tree from the first value GE than the from
	NewIterator(treeID graveler.TreeID, from graveler.Key) (Iterator, error)

	// GetIterForPart accepts a tree ID and a reading start point. it returns an iterator
	// positioned at the start point. When Next() will be called, first value that is GE
	// than the from key will be returned
	NewPartIterator(partID committed.ID, from graveler.Key) (graveler.ValueIterator, error)
}

// Writer is an abstraction for creating new trees
type Writer interface {
	// WriteRecord adds a record to the tree. The key must be greater than any other key that was written
	// (in other words - values must be entered sorted by key order).
	// If the most recent insertion was using AddParts, the key must be greater than any key in the added parts.
	WriteRecord(record graveler.ValueRecord) error

	// AddPart adds a complete part to the tree at the current insertion point.
	// Added part must not contain keys smaller than last previously written value.
	AddPart(part Part) error

	// Close finalizes the tree creation. It's invalid to add records after calling this method.
	// During tree writing, parts are closed asynchronously and copied by tierFS
	// while writing continues. SaveTree waits until closing and copying all parts.
	Close() (*graveler.TreeID, error)

	Abort() error
}
