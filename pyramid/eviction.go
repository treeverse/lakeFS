package pyramid

import (
	"fmt"

	"github.com/dgraph-io/ristretto"
)

type evictionControl struct {
	cache *ristretto.Cache
}

func newEvictionControl(capacity, estimatedFileSize int64, evict func(filename string)) (*evictionControl, error) {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 10 * capacity / estimatedFileSize,
		MaxCost:     capacity,
		Metrics:     false,
		OnEvict:     onEvict(evict),
	})
	if err != nil {
		return nil, fmt.Errorf("creating cache: %w", err)
	}
	return &evictionControl{
		cache: cache,
	}, nil
}

func onEvict(evict func(localpath string)) func(uint64, uint64, interface{}, int64) {
	return func(_, _ uint64, value interface{}, _ int64) {
		evict(value.(string))
	}
}

// touch updates last access time for the file
func (am *evictionControl) touch(localpath string) {
	am.cache.Get(localpath)
}

func (am *evictionControl) store(localpath string, filesize int64) {
	am.cache.Set(localpath, localpath, filesize)
}
