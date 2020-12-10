package sstable

import "github.com/cockroachdb/pebble/sstable"

// Derefer is the type of a function that returns an object to the cache, possibly causing
// its eviction.
type Derefer func() error

type cache interface {
	GetOrOpen(namespace string, id ID) (*sstable.Reader, Derefer, error)
}
