package cache

import (
	"errors"
	"math/rand"
	"time"

	lru "github.com/hnlq715/golang-lru"
)

type JitterFn func() time.Duration
type SetFn func() (v interface{}, err error)

type Cache interface {
	GetOrSet(k interface{}, setFn SetFn) (v interface{}, err error)
}

type GetSetCache struct {
	lru        *lru.Cache
	locker     *ChanLocker
	jitterFn   JitterFn
	baseExpiry time.Duration
}

var (
	ErrCacheItemNotFound = errors.New("cache item not found")
)

func NewCache(size int, expiry time.Duration, jitterFn JitterFn) *GetSetCache {
	c, _ := lru.New(size)
	return &GetSetCache{
		lru:        c,
		locker:     NewChanLocker(),
		jitterFn:   jitterFn,
		baseExpiry: expiry,
	}
}

func (c *GetSetCache) GetOrSet(k interface{}, setFn SetFn) (v interface{}, err error) {
	if v, ok := c.lru.Get(k); ok {
		return v, nil
	}
	acquired := c.locker.Lock(k, func() {
		v, err = setFn()
		if err != nil {
			return
		}
		c.lru.AddEx(k, v, c.baseExpiry+c.jitterFn())
	})
	if acquired {
		return v, err
	}

	// someone else got the lock first and should have inserted something
	if v, ok := c.lru.Get(k); ok {
		return v, nil
	}

	// someone else acquired the lock, but no key was found
	// (most likely this value doesn't exist or the upstream fetch failed)
	return nil, ErrCacheItemNotFound
}

func NewJitterFn(jitter time.Duration) JitterFn {
	return func() time.Duration {
		n := rand.Intn(int(jitter)) //nolint:gosec
		return time.Duration(n)
	}
}
