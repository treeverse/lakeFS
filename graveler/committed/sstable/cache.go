package sstable

import (
	"fmt"

	"github.com/cockroachdb/pebble/sstable"
	lru "github.com/treeverse/lakefs/cache"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/pyramid"
)

//go:generate mockgen -source=cache.go -destination=mock/cache.go -package=mock

type Derefer lru.Derefer

type opener func(namespace string, id committed.ID) (Item, error)

type cache interface {
	GetOrOpen(namespace string, id committed.ID) (*sstable.Reader, Derefer, error)
}

type lruCache struct {
	c    lru.CacheWithDisposal
	open opener
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

func NewCache(p lru.ParamsWithDisposal, fs pyramid.FS, readerOptions sstable.ReaderOptions) cache {
	return NewCacheWithOpener(p, func(namespace string, id committed.ID) (Item, error) {
		file, err := fs.Open(namespace, string(id))
		if err != nil {
			return nil, fmt.Errorf("fetch %s from next tier: %w", id, err)
		}

		reader, err := sstable.NewReader(file, readerOptions)
		if err != nil {
			return nil, fmt.Errorf("open SSTable %s: %w", id, err)
		}
		return &item{reader}, nil
	})
}

func NewCacheWithOpener(p lru.ParamsWithDisposal, open opener) cache {
	if p.OnDispose != nil {
		panic("external OnDispose not supported for sstable cache")
	}
	p.OnDispose = func(v interface{}) error {
		item := v.(Item)
		return item.Close()
	}
	return &lruCache{
		c:    lru.NewCacheWithDisposal(p),
		open: open,
	}
}

type namespaceID struct {
	namespace string
	id        committed.ID
}

func (c *lruCache) GetOrOpen(namespace string, id committed.ID) (*sstable.Reader, Derefer, error) {
	e, derefer, err := c.c.GetOrSet(namespaceID{namespace, id}, func() (interface{}, error) {
		r, err := c.open(namespace, id)
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
