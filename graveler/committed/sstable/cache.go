package sstable

import (
	"github.com/cockroachdb/pebble/sstable"
	"github.com/treeverse/lakefs/graveler/committed"
)

//go:generate mockgen -source=cache.go -destination=mock/cache.go -package=mock

type cache interface {
	// derefer is the type of a function that returns an object to the cache, possibly causing
	// its eviction.
	GetOrOpen(namespace string, id committed.ID) (reader *sstable.Reader, derefer func() error, err error)
}
