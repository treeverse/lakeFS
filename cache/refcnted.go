package cache

import (
	"errors"
	"fmt"
	"hash/maphash"
	"sync"
	"sync/atomic"

	"github.com/hnlq715/golang-lru/simplelru"

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
	Name   string
	Logger logging.Logger
	Size   int
	Shards int
	// OnDispose disposes of an entry.  It is called when the last reference to an entry
	// is released.  If it fails not much can be done.
	OnDispose func(value interface{}) error
}

// shardedCacheWithDisposal shards a CacheWithDisposal across its keys.  It requires that its
// keyus have a String() method that is compatible with their equality, i.e. k1.GoString() ==
// k2.GoString() implies k1 equals k2 in the sense of map comparison.
type shardedCacheWithDisposal struct {
	name   string
	Seed   maphash.Seed
	Shards []CacheWithDisposal
}

func NewCacheWithDisposal(p ParamsWithDisposal) *shardedCacheWithDisposal {
	if p.Shards <= 0 {
		panic("need at least 1 shard")
	}
	shards := make([]CacheWithDisposal, p.Shards)
	for i := 0; i < p.Shards; i++ {
		shards[i] = NewSingleThreadedCacheWithDisposal(p)
	}
	return &shardedCacheWithDisposal{Seed: maphash.MakeSeed(), Shards: shards}
}

func (s *shardedCacheWithDisposal) Name() string {
	return s.name
}

func (s *shardedCacheWithDisposal) GetOrSet(k interface{}, setFn SetFn) (interface{}, Derefer, error) {
	hash := maphash.Hash{}
	hash.SetSeed(s.Seed)
	// (Explicitly ignore return value from hash.WriteString: its godoc says "it always
	// writes all of s and never fails; the count and error result are for implementing
	// io.StringWriter."
	_, _ = hash.WriteString(fmt.Sprintf("%+#v", k))
	hashVal := hash.Sum64() % uint64(len(s.Shards))
	return s.Shards[hashVal].GetOrSet(k, setFn)
}

// SingleThreadedCacheWithDisposal is a CacheWithDisposal that uses a single critical section
// to prevent concurrent access to the cache and to reference counts on values.
type SingleThreadedCacheWithDisposal struct {
	p    *ParamsWithDisposal
	name string
	mu   sync.Mutex // protects lru
	lru  *simplelru.LRU
}

// cacheEntry is a single entry in the cache.  It uses only atomic operations on refs and is
// safe for concurrent use.
type cacheEntry struct {
	refs  int32
	value interface{}
}

var ErrNegativeReferenceCount = errors.New("internal error: negative reference count")

// release releases one reference count from e, releasing it from cd if that was the last
// reference.
func (e *cacheEntry) release(c *SingleThreadedCacheWithDisposal) error {
	refs := atomic.AddInt32(&e.refs, -1)
	if refs < 0 {
		return fmt.Errorf("release from %s: %w %d; may leak or fail", c.Name(), ErrNegativeReferenceCount, refs)
	}
	if refs > 0 {
		return nil
	}

	return c.p.OnDispose(e.value)
}

// acquire acquires another reference on e.
func (e *cacheEntry) acquire() {
	atomic.AddInt32(&e.refs, +1)
}

func (c *SingleThreadedCacheWithDisposal) Name() string {
	return c.name
}

func NewSingleThreadedCacheWithDisposal(p ParamsWithDisposal) *SingleThreadedCacheWithDisposal {
	ret := &SingleThreadedCacheWithDisposal{
		name: p.Name,
	}
	onEvict := func(k interface{}, v interface{}) {
		entry := v.(*cacheEntry)
		err := entry.release(ret)
		p.Logger.WithField("key", k).WithError(err).Error("[internal] failed to release during eviction")
	}
	ret.p = &p
	var err error
	ret.lru, err = simplelru.NewLRU(p.Size, onEvict)
	if err != nil {
		panic(err)
	}
	return ret
}

func (c *SingleThreadedCacheWithDisposal) GetOrSet(k interface{}, setFn SetFn) (interface{}, Derefer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var entry *cacheEntry
	if e, ok := c.lru.Get(k); ok {
		entry = e.(*cacheEntry)
		entry.acquire()
	} else {
		v, err := setFn()
		if err != nil {
			return nil, nil, err
		}
		entry = &cacheEntry{refs: 1, value: v}
	}
	return entry.value, func() error {
		return entry.release(c)
	}, nil
}
