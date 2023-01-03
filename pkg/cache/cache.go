package cache

import (
	"math/rand"
	"time"

	lru "github.com/hnlq715/golang-lru"
)

type (
	JitterFn func() time.Duration
	SetFn    func() (v interface{}, err error)
)

type Cache interface {
	GetOrSet(k interface{}, setFn SetFn) (v interface{}, err error)
	Set(k, v interface{})
	Clear(k interface{})
}

type GetSetCache struct {
	lru          *lru.Cache
	computations *ChanOnlyOne
	jitterFn     JitterFn
	baseExpiry   time.Duration
}

func NewCache(size int, expiry time.Duration, jitterFn JitterFn) *GetSetCache {
	c, _ := lru.New(size)
	return &GetSetCache{
		lru:          c,
		computations: NewChanOnlyOne(),
		jitterFn:     jitterFn,
		baseExpiry:   expiry,
	}
}

func (c *GetSetCache) GetOrSet(k interface{}, setFn SetFn) (v interface{}, err error) {
	if v, ok := c.lru.Get(k); ok {
		return v, nil
	}
	return c.computations.Compute(k, func() (interface{}, error) {
		v, err = setFn()
		if err != nil { // Don't cache errors
			return nil, err
		}
		_ = c.lru.AddEx(k, v, c.baseExpiry+c.jitterFn())
		return v, nil
	})
}

func (c *GetSetCache) Set(k, v interface{}) {
	_ = c.lru.AddEx(k, v, c.baseExpiry+c.jitterFn())
}

func (c *GetSetCache) Clear(k interface{}) {
	c.lru.Remove(k)
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
