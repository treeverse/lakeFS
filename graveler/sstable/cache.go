package sstable

import (
	"context"
	"fmt"

	lru "github.com/treeverse/lakefs/cache"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/pyramid"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
)

//go:generate mockgen -source=cache.go -destination=mock/cache.go -package=mock

const DefaultPebbleSSTableCacheSize = 10_000_000

type Derefer lru.Derefer

type opener = func(ctx context.Context, namespace string, filename string) (Item, error)

type existser = func(ctx context.Context, namespace string, filename string) (bool, error)

type Cache interface {
	// GetOrOpen returns a reader for id in namespace ns, and a Derefer which must be
	// called to release the reader.
	GetOrOpen(ctx context.Context, namespace string, id committed.ID) (*sstable.Reader, Derefer, error)
	// Exists returns true if id exists in namespace ns and could be fetched.  It ignores
	// all caching.
	Exists(ctx context.Context, namespace string, id committed.ID) (bool, error)
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
	if readerOptions.Cache == nil {
		// Shared cache between *all* readers, otherwise sstable.NewReader keeps its
		// refcnt but forgets only reference.  *This* cache is held inside opener so
		// can never be finalized.
		readerOptions.Cache = pebble.NewCache(DefaultPebbleSSTableCacheSize)
		defer readerOptions.Cache.Unref()
	}
	opener := makePyramidOpener(fs, readerOptions)
	if p.Size == 0 {
		return &noCache{opener: opener}
	}
	return NewCacheWithOpener(p, opener, fs.Exists)
}

func makePyramidOpener(fs pyramid.FS, readerOptions sstable.ReaderOptions) opener {
	if readerOptions.Cache != nil { // Returned opener closure refers to readerOptions.Cache
		readerOptions.Cache.Ref()
	}

	return func(ctx context.Context, namespace string, id string) (Item, error) {
		file, err := fs.Open(ctx, namespace, id)
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

func (c *lruCache) GetOrOpen(ctx context.Context, namespace string, id committed.ID) (*sstable.Reader, Derefer, error) {
	e, derefer, err := c.c.GetOrSet(namespaceID{namespace, id}, func() (interface{}, error) {
		r, err := c.open(ctx, namespace, string(id))
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

func (c *lruCache) Exists(ctx context.Context, namespace string, id committed.ID) (bool, error) {
	return c.exists(ctx, namespace, string(id))
}

type noCache struct {
	opener opener
}

func (n *noCache) GetOrOpen(ctx context.Context, namespace string, id committed.ID) (*sstable.Reader, Derefer, error) {
	item, err := n.opener(ctx, namespace, string(id))
	if err != nil {
		return nil, nil, err
	}
	reader := item.GetSSTable()
	return reader, reader.Close, nil
}

func (n *noCache) Exists(ctx context.Context, namespace string, id committed.ID) (bool, error) {
	item, err := n.opener(ctx, namespace, string(id))
	if err != nil {
		return false, err
	}
	reader := item.GetSSTable()
	return true, reader.Close()
}
