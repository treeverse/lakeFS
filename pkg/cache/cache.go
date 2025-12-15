package cache

import (
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
)

type Cache[K comparable, V any] interface {
	GetOrSet(k K, setFn SetFn[V]) (v V, err error)
}

type SetFn[V any] func() (v V, err error)

type GetSetCache[K comparable, V any] struct {
	lru    *lru.LRU[K, V]
	expiry time.Duration
}

func NewCache[K comparable, V any](size int, expiry time.Duration) *GetSetCache[K, V] {
	return &GetSetCache[K, V]{
		lru:    lru.NewLRU[K, V](size, nil, expiry),
		expiry: expiry,
	}
}

func (c *GetSetCache[K, V]) GetOrSet(k K, setFn SetFn[V]) (V, error) {
	if v, ok := c.lru.Get(k); ok {
		return v, nil
	}
	v, err := setFn()
	if err != nil { // Don't cache errors
		return v, err
	}

	c.lru.Add(k, v)
	return v, nil
}
