package pyramid

import (
	"fmt"

	"github.com/dgraph-io/ristretto"
)

// evictionControl is in charge of limiting the size of local files in TierFS.
type evictionControl interface {
	// touch updates the last time the path was read.
	touch(rPath relativePath)

	// store updates the eviction control with the path and the file size.
	store(rPath relativePath, filesize int64)
}

type lruSizeEviction struct {
	cache *ristretto.Cache
}

func newLRUSizeEviction(capacity, estimatedFileBytes int64, evict func(rPath relativePath)) (*lruSizeEviction, error) {
	cache, err := ristretto.NewCache(&ristretto.Config{
		// Per ristretto, this is the optimized counters num
		NumCounters: int64(10.0 * float64(capacity) / float64(estimatedFileBytes)),
		MaxCost:     capacity,
		Metrics:     false,
		OnEvict:     onEvict(evict),
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
	am.cache.Get(rPath)
}

func (am *lruSizeEviction) store(rPath relativePath, filesize int64) {
	am.cache.Set(rPath, rPath, filesize)
}
