package cache

import (
	"errors"
	"math/rand"
	"time"

	lru "github.com/hnlq715/golang-lru"
)

type JitterFn func() time.Duration
type SetFn func() (v interface{}, err error)
type EvictionCallback func(key interface{}, value interface{})

// Params controls a Cache.
type Params struct {
	// User-visible name to give this cache.
	Name string
	// Size is the maximal number of SSTables to hold open.  Each
	// SSTable consumes an open file descriptor and possibly some
	// memory.
	Size int
	// Expiry is an extra time to keep elements in cache before eviction.
	Expiry time.Duration
	// Jitter is the interval to jitter around expiry.
	JitterFn JitterFn
	// OnEvict is called after an element has been evicted from the cache.
	OnEvict EvictionCallback
}

type Cache interface {
	Name() string
	GetOrSet(k interface{}, setFn SetFn) (v interface{}, err error)
}

type GetSetCache struct {
	p      *Params
	lru    *lru.Cache
	locker *ChanLocker
}

var (
	ErrCacheItemNotFound = errors.New("cache item not found")
)

func NewCache(size int, expiry time.Duration, jitterFn JitterFn) *GetSetCache {
	return NewCacheByParams(&Params{Size: size, Expiry: expiry, JitterFn: jitterFn})
}

func NewCacheByParams(p *Params) *GetSetCache {
	// TODO(ozkatz): Handle error return.
	c, err := lru.NewWithEvict(p.Size, p.OnEvict)
	if err != nil {
		panic(err)
	}
	return &GetSetCache{
		lru:    c,
		locker: NewChanLocker(),
		p:      p,
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
		c.lru.AddEx(k, v, c.p.Expiry+c.p.JitterFn())
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

func (c *GetSetCache) Name() string { return c.p.Name }

func NewJitterFn(jitter time.Duration) JitterFn {
	return func() time.Duration {
		n := rand.Intn(int(jitter)) //nolint:gosec
		return time.Duration(n)
	}
}
