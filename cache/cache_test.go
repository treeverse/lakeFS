package cache_test

import (
	"sync"
	"testing"
	"time"

	"github.com/treeverse/lakefs/cache"
)

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
