package cache

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/treeverse/lakefs/logging"
)

// Derefer is the type of a function that returns an object to the cache, possibly causing
// its eviction.
type Derefer func() error

// CacheWithDisposal is a cache that calls a disposal callback when an entry has been evicted
// and is no longer in use.  Because of the structure of Go, it uses release callbacks and is
// not a Cache.
type CacheWithDisposal interface {
	Name() string
	GetOrSet(k interface{}, setFn SetFn) (v interface{}, disposer Derefer, err error)
}

type ParamsWithDisposal struct {
	Params
	// OnDispose disposes of an entry.  It is called when the last reference to an entry
	// is released.  If it fails not much can be done.
	OnDispose func(value interface{}) error
}

// ReferenceCountedCache is a CacheWithDisposal implemented as reference counting on top of
// Cache.
type ReferenceCountedCache struct {
	p *ParamsWithDisposal
	c Cache
}

type cacheEntry struct {
	refs  int32
	value interface{}
}

var ErrNegativeReferenceCount = errors.New("internal error: negative reference count")

// release releases one reference count from e, releasing it from cd if that was the last
// reference.
func (e *cacheEntry) release(rcc *ReferenceCountedCache) error {
	refs := atomic.AddInt32(&e.refs, -1)
	if refs < 0 {
		return fmt.Errorf("release from %s: %w %d; may leak or fail", rcc.Name(), ErrNegativeReferenceCount, refs)
	}
	if refs > 0 {
		return nil
	}

	return rcc.p.OnDispose(e.value)
}

func NewCacheWithDisposal(p *ParamsWithDisposal) *ReferenceCountedCache {
	disposingParams := *p
	ret := &ReferenceCountedCache{}
	disposingParams.OnEvict = func(k interface{}, v interface{}) {
		entry := v.(*cacheEntry)
		err := entry.release(ret)
		logging.Default().WithFields(logging.Fields{
			"key":   k,
			"value": v,
		}).WithError(err).Error("[internal] failed to release during eviction")
		p.OnEvict(k, v)
	}
	ret.p = &disposingParams
	ret.c = NewCacheByParams(&p.Params)
	return ret
}

func (rcc *ReferenceCountedCache) Name() string {
	return rcc.c.Name()
}

func (rcc *ReferenceCountedCache) GetOrSet(k interface{}, setFn SetFn) (interface{}, Derefer, error) {
	e, err := rcc.c.GetOrSet(k, func() (interface{}, error) {
		v, err := setFn()
		var ret *cacheEntry
		if v != nil {
			ret = &cacheEntry{refs: 1, value: v}
		}
		return ret, err
	})
	entry := e.(*cacheEntry)
	return entry.value, func() error {
		return entry.release(rcc)
	}, err
}
