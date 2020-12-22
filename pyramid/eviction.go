package pyramid

import (
	"fmt"

	lru "github.com/treeverse/golang-lru"
	"github.com/treeverse/golang-lru/simplelru"
)

// eviction is an abstraction of the eviction control for easy testing
type eviction interface {
	touch(rPath relativePath)
	store(rPath relativePath, filesize int64) int
}

type lruSizeEviction struct {
	cache simplelru.LRUCache
}

func newLRUSizeEviction(capacity int64, evict func(rPath relativePath, cost int64)) (eviction, error) {
	cache, err := lru.NewWithEvict(capacity, func(key interface{}, _ interface{}, cost int64) {
		evict(key.(relativePath), cost)
	})
	if err != nil {
		return nil, fmt.Errorf("creating cache: %w", err)
	}
	return &lruSizeEviction{
		cache: cache,
	}, nil
}

func (am *lruSizeEviction) touch(rPath relativePath) {
	// update last access time, value is meaningless
	am.cache.Get(rPath)
}

func (am *lruSizeEviction) store(rPath relativePath, filesize int64) int {
	return am.cache.Add(rPath, nil, filesize)
}
