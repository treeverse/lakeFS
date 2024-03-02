package cache

import (
	"math/rand"
	"time"

	lru "github.com/hnlq715/golang-lru"
)

type JitterFn func() time.Duration
type SetFn func() (v interface{}, err error)

// SetWithExpiry is a function called to set a value in the cache.  It
// returns the desired value and when to expire it from the cache.  The
// cache default expiration value is used if it returns a zero expiration.
type SetFnWithExpiry func() (v interface{}, expiry time.Duration, err error)

type Cache interface {
	GetOrSet(k interface{}, setFn SetFn) (v interface{}, err error)
	GetOrSetWithExpiry(k interface{}, setFn SetFnWithExpiry) (v interface{}, err error)
}

type GetSetCache struct {
	lru          *lru.Cache
	computations *ChanOnlyOne
	expiry       time.Duration
	jitterFn     JitterFn
}

func NewCache(size int, expiry time.Duration, jitterFn JitterFn) *GetSetCache {
	c, _ := lru.New(size)
	return &GetSetCache{
		lru:          c,
		computations: NewChanOnlyOne(),
		expiry:       expiry,
		jitterFn:     jitterFn,
	}
}

func (c *GetSetCache) GetOrSet(k interface{}, setFn SetFn) (v interface{}, err error) {
	setFnWithDefaultExpiry := SetFnWithExpiry(func() (interface{}, time.Duration, error) {
		v, err := setFn()
		return v, 0, err
	})
	return c.GetOrSetWithExpiry(k, setFnWithDefaultExpiry)
}

func (c *GetSetCache) GetOrSetWithExpiry(k interface{}, setFn SetFnWithExpiry) (v interface{}, err error) {
	if v, ok := c.lru.Get(k); ok {
		return v, nil
	}
	return c.computations.Compute(k, func() (interface{}, error) {
		v, expiry, err := setFn()
		if err != nil { // Don't cache errors
			return nil, err
		}
		if expiry == 0 {
			expiry = c.expiry + c.jitterFn()
		}
		c.lru.AddEx(k, v, expiry)
		return v, nil
	})
}

func NewJitterFn(jitter time.Duration) JitterFn {
	if jitter <= 0 {
		return func() time.Duration {
			return 0
		}
	}
	return func() time.Duration {
		n := rand.Intn(int(jitter)) //nolint:gosec
		return time.Duration(n)
	}
}
