package pyramid

import (
	"fmt"

	"github.com/dgraph-io/ristretto"
)

type lruSizeEviction struct {
	cache *ristretto.Cache
}

const (
	// Per ristretto, this is the optimized (static) buffer items
	bufferItems = 64
)

func newLRUSizeEviction(capacity, estimatedFileBytes int64, evict func(rPath relativePath)) (*lruSizeEviction, error) {
	cache, err := ristretto.NewCache(&ristretto.Config{
		// Per ristretto, this is the optimized counters num
		NumCounters: int64(10.0 * float64(capacity) / float64(estimatedFileBytes)),
		MaxCost:     capacity,
		Metrics:     false,
		OnEvict:     onEvict(evict),
		BufferItems: bufferItems,
	})
	if err != nil {
		return nil, fmt.Errorf("creating cache: %w", err)
	}
	return &lruSizeEviction{
		cache: cache,
	}, nil
}

func onEvict(evict func(rPath relativePath)) func(uint64, uint64, interface{}, int64) {
	return func(_, _ uint64, value interface{}, _ int64) {
		evict(value.(relativePath))
	}
}

func (am *lruSizeEviction) touch(rPath relativePath) {
	am.cache.Get(string(rPath))
}

func (am *lruSizeEviction) store(rPath relativePath, filesize int64) bool {
	// must store the path as value since the returned key is the hash,
	// not the actual key.
	return am.cache.Set(string(rPath), rPath, filesize)
}
