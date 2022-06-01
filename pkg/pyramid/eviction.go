package pyramid

import (
	"fmt"

	"github.com/dgraph-io/ristretto"
	"github.com/treeverse/lakefs/pkg/pyramid/params"
)

//nolint: unused
type ristrettoEviction struct {
	cache         *ristretto.Cache
	evictCallback func(rPath params.RelativePath, cost int64)
}

const (
	// 10M for an efficient 1M stored items (less than 5MB overhead)
	numCounters = 10_000_000

	// 64 is the recommended buffer-items for all use-cases
	bufferItems = 64
)

//nolint: unused,deadcode
func newRistrettoEviction(capacity int64, evict func(rPath params.RelativePath, cost int64)) (params.Eviction, error) {
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
