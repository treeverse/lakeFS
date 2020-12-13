package cache_test

import (
	"sync"
	"testing"
	"time"

	"github.com/treeverse/lakefs/cache"
	"github.com/treeverse/lakefs/testutil"
)

func TestCache(t *testing.T) {
	const (
		n = 200
		// Thrash the cache by placing worldSize-1 every even iteration and the
		// remaining values ever odd iteration.  In particular must have cacheSize <
		// worldSize-1.
		worldSize = 10
		cacheSize = 7
	)

	c := cache.NewCache(cacheSize, time.Hour*12, cache.NewJitterFn(time.Millisecond))

	numCalls := 0
	for i := 0; i < n; i++ {
		var k int
		if i%2 == 0 {
			k = worldSize - 1
		} else {
			k = (i / 2) % (worldSize - 1)
		}
		actual, err := c.GetOrSet(k, func() (interface{}, error) {
			numCalls++
			return k * k, nil
		})
		testutil.MustDo(t, "GetOrSet", err)
		if actual.(int) != k*k {
			t.Errorf("got %v != %d at %d", actual, k*k, k)
		}
	}
	// Every even call except the first is served from cache; no odd call is ever served
	// from cache.
	expectedNumCalls := 1 + n/2
	if numCalls != expectedNumCalls {
		t.Errorf("cache called refill %d times instead of %d", numCalls, expectedNumCalls)
	}
}

func TestCacheRace(t *testing.T) {
	const (
		parallelism = 25
		n           = 200
		worldSize   = 10
		cacheSize   = 7
	)

	c := cache.NewCache(cacheSize, time.Hour*12, cache.NewJitterFn(time.Millisecond))

	start := make(chan struct{})
	wg := sync.WaitGroup{}

	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func(i int) {
			<-start
			for j := 0; j < n; j++ {
				k := j % worldSize
				kk, err := c.GetOrSet(k, func() (interface{}, error) {
					return k * k, nil
				})
				if err != nil {
					t.Error(err)
					return
				}
				if kk.(int) != k*k {
					t.Errorf("[%d] got %d^2=%d, expected %d", i, k, kk, k*k)
				}
			}
			wg.Done()
		}(i)
	}
	close(start)
	wg.Wait()
}
