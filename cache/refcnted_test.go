package cache_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/treeverse/lakefs/cache"
)

func TestCacheWithDisposal(t *testing.T) {
	const (
		size        = 7
		parallelism = 100
		repeats     = 2000
	)
	var disposed [size]int32
	ch := make(chan error, parallelism*repeats)
	p := cache.ParamsWithDisposal{
		Params: cache.Params{
			Name:     t.Name(),
			Size:     size,
			Expiry:   time.Hour * 999999,
			JitterFn: func() time.Duration { return time.Duration(0) },
			OnEvict:  nil,
		},
		OnDispose: func(v interface{}) error {
			i := v.(int)
			if n := atomic.AddInt32(&disposed[i], 1); n != 1 {
				ch <- fmt.Errorf("%d disposals of %d", n, i)
			}
			return nil
		},
	}
	c := cache.NewCacheWithDisposal(&p)

	numCreated := int32(0)

	wg := sync.WaitGroup{}
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func(i int) {
			for j := 0; j < repeats; j++ {
				k := j % size
				v, release, err := c.GetOrSet(
					k, func() (interface{}, error) {
						atomic.AddInt32(&numCreated, int32(1))
						return k, nil
					})
				if err != nil {
					ch <- err
				}
				if v.(int) != k {
					ch <- fmt.Errorf("got %v not %d", v, k)
				}
				if (j*repeats+i)%113 == 17 {
					time.Sleep(17 * time.Millisecond)
				}
				release()
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	close(ch)
	for err := range ch {
		t.Error(err)
	}
}
