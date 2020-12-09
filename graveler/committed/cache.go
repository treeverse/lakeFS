package sstable

import (
	"fmt"

	"github.com/cockroachdb/pebble/sstable"
	lru "github.com/treeverse/lakefs/cache"
	"github.com/treeverse/lakefs/pyramid"
)

type Derefer lru.Derefer

type cache struct {
	c             lru.CacheWithDisposal
	fs            pyramid.FS
	readerOptions sstable.ReaderOptions
}

func NewCache(p lru.Params, fs pyramid.FS, readerOptions sstable.ReaderOptions) *cache {
	return &cache{
		c: lru.NewCacheWithDisposal(&lru.ParamsWithDisposal{
			Params: p,
			OnDispose: func(v interface{}) error {
				sst := v.(*sstable.Reader)
				return sst.Close()
			},
		}),
		fs:            fs,
		readerOptions: readerOptions,
	}
}

func (c *cache) GetOrOpen(namespace string, id ID) (*sstable.Reader, Derefer, error) {
	e, derefer, err := c.c.GetOrSet(id, func() (interface{}, error) {
		file, err := c.fs.Open(namespace, string(id))
		if err != nil {
			return nil, fmt.Errorf("fetch %s from next tier: %w", id, err)
		}
		r, err := sstable.NewReader(file, c.readerOptions)
		if err != nil {
			return nil, fmt.Errorf("open SSTable %s after fetch from next tier: %w", id, err)
		}
		return r, nil
	})
	if err != nil {
		return nil, nil, err
	}
	return e.(*sstable.Reader), Derefer(derefer), err
}
