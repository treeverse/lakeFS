package sstable

import (
	"fmt"

	lru "github.com/treeverse/lakefs/cache"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/pyramid"

	"github.com/cockroachdb/pebble/sstable"
)

//go:generate mockgen -source=cache.go -destination=mock/cache.go -package=mock

type Derefer lru.Derefer

type opener = func(namespace string, filename string) (Item, error)

type existser = func(namespace string, filename string) (bool, error)

type Cache interface {
	// GetOrOpen returns a reader for id in namespace ns, and a Derefer which must be
	// called to release the reader.
	GetOrOpen(namespace string, id committed.ID) (*sstable.Reader, Derefer, error)
	// Exists returns true if id exists in namespace ns and could be fetched.  It ignores
	// all caching.
	Exists(namespace string, id committed.ID) (bool, error)
}

type lruCache struct {
	c      lru.CacheWithDisposal
	open   opener
	exists existser
}

// item holds an SSTable inside a cache.  It exists (only) to allow tests to replace its
// closer with a testable fake.
type Item interface {
	GetSSTable() *sstable.Reader
	Close() error
}

type item struct {
	r *sstable.Reader
}

func (i *item) GetSSTable() *sstable.Reader {
	return i.r
}

func (i *item) Close() error {
	return i.r.Close()
}

func NewCache(p lru.ParamsWithDisposal, fs pyramid.FS, readerOptions sstable.ReaderOptions) Cache {
	opener := makePyramidOpener(fs, readerOptions)
	if p.Size == 0 {
		return &noCache{opener: opener}
	}
	return NewCacheWithOpener(p, opener, fs.Exists)
}

func makePyramidOpener(fs pyramid.FS, readerOptions sstable.ReaderOptions) opener {
	return func(namespace string, id string) (Item, error) {
		file, err := fs.Open(namespace, id)
		if err != nil {
			return nil, fmt.Errorf("fetch %s from next tier: %w", id, err)
		}

		reader, err := sstable.NewReader(file, readerOptions)
		if err != nil {
			return nil, fmt.Errorf("open SSTable %s: %w", id, err)
		}
		return &item{reader}, nil
	}
}

func NewCacheWithOpener(p lru.ParamsWithDisposal, open opener, exists existser) Cache {
	if p.OnDispose != nil {
		panic("external OnDispose not supported for sstable cache")
	}
	p.OnDispose = func(v interface{}) error {
		item := v.(Item)
		return item.Close()
	}
	return &lruCache{
		c:      lru.NewCacheWithDisposal(p),
		open:   open,
		exists: exists,
	}
}

type namespaceID struct {
	namespace string
	id        committed.ID
}

func (c *lruCache) GetOrOpen(namespace string, id committed.ID) (*sstable.Reader, Derefer, error) {
	e, derefer, err := c.c.GetOrSet(namespaceID{namespace, id}, func() (interface{}, error) {
		r, err := c.open(namespace, string(id))
		if err != nil {
			return nil, fmt.Errorf("open SSTable %s after fetch from next tier: %w", id, err)
		}
		return r, nil
	})
	if err != nil {
		return nil, nil, err
	}
	item := e.(Item)
	return item.GetSSTable(), Derefer(derefer), err
}

func (c *lruCache) Exists(namespace string, id committed.ID) (bool, error) {
	return c.exists(namespace, string(id))
}

type noCache struct {
	opener opener
}

func (n *noCache) GetOrOpen(namespace string, id committed.ID) (*sstable.Reader, Derefer, error) {
	item, err := n.opener(namespace, string(id))
	if err != nil {
		return nil, nil, err
	}
	reader := item.GetSSTable()
	return reader, reader.Close, nil
}

func (n *noCache) Exists(string, committed.ID) (bool, error) {
	return false, nil
}
