package pyramid

import (
	"fmt"

	"github.com/dgraph-io/ristretto"
)

// eviction is an abstraction of the eviction control for easy testing
type eviction interface {
	touch(rPath relativePath)
	store(rPath relativePath, filesize int64) bool
}

type ristrettoEviction struct {
	cache         *ristretto.Cache
	evictCallback func(rPath relativePath, cost int64)
}

const (
	// 10M for an efficient 1M stored items (less than 5MB overhead)
	numCounters = 10000000

	// 64 is the recommended buffer-items for all use-cases
	bufferItems = 64
)

func newRistrettoEviction(capacity int64, evict func(rPath relativePath, cost int64)) (eviction, error) {
	re := &ristrettoEviction{evictCallback: evict}

	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: numCounters,
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

func (re *ristrettoEviction) touch(rPath relativePath) {
	// update last access time, value is meaningless
	re.cache.Get(string(rPath))
}

func (re *ristrettoEviction) store(rPath relativePath, filesize int64) bool {
	// setting the path as the value since only the key hash is returned
	// to the onEvict callback
	return re.cache.Set(string(rPath), rPath, filesize)
}

func (re *ristrettoEviction) onEvict(item *ristretto.Item) {
	re.evictCallback(item.Value.(relativePath), item.Cost)
}
