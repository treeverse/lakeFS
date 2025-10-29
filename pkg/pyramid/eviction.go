package pyramid

import (
	"fmt"

	"github.com/dgraph-io/ristretto"
	"github.com/treeverse/lakefs/pkg/pyramid/params"
)

// nolint: unused
type ristrettoEviction struct {
	cache         *ristretto.Cache
	evictCallback func(rPath params.RelativePath, cost int64)
}

const (
	// 10M for an efficient 1M stored items (less than 5MB overhead)
	numCounters = 10_000_000

	// 64 is the recommended buffer-items for all use-cases
	bufferItems = 64

	// Cache size threshold for adaptive counter scaling
	smallCacheThreshold = 100 * 1024 * 1024 // 100MB

	// Counter scaling parameters for small caches
	bytesPerCounter = 1024 * 10 // 10KB per counter (~100 counters per 1MB)
	minCounters     = 1000      // Minimum counters to ensure basic functionality
)

//nolint:unused
func newRistrettoEviction(capacity int64, evict func(rPath params.RelativePath, cost int64)) (params.Eviction, error) {
	re := &ristrettoEviction{evictCallback: evict}

	// Scale numCounters based on capacity to avoid excessive memory usage for small caches
	// Default 10M counters is too much for test environments with small cache sizes
	var numCountersToUse int64 = numCounters
	if capacity < smallCacheThreshold {
		// Use a more reasonable ratio: ~100 counters per 1MB of capacity
		numCountersToUse = capacity / bytesPerCounter
		if numCountersToUse < minCounters {
			numCountersToUse = minCounters
		}
		if numCountersToUse > numCounters {
			numCountersToUse = numCounters // Don't exceed default for large caches
		}
	}

	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: numCountersToUse,
		MaxCost:     capacity,
		BufferItems: bufferItems,
		OnEvict:     re.onEvict,
		OnReject:    re.onEvict,
	})
	if err != nil {
		return nil, fmt.Errorf("creating ristretto cache: %w", err)
	}

	re.cache = cache
	return re, nil
}

func (re *ristrettoEviction) Touch(rPath params.RelativePath) {
	// update last access time, value is meaningless
	re.cache.Get(string(rPath))
}

func (re *ristrettoEviction) Store(rPath params.RelativePath, filesize int64) bool {
	// setting the path as the value since only the key hash is returned
	// to the onEvict callback
	return re.cache.Set(string(rPath), rPath, filesize)
}

func (re *ristrettoEviction) onEvict(item *ristretto.Item) {
	if item.Value != nil {
		re.evictCallback(item.Value.(params.RelativePath), item.Cost)
	}
}
