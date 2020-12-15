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
	type record struct {
		key      int
		disposed int32
		live     int32
	}
	var elements [size]*record
	ch := make(chan error, parallelism*2)
	allErrs := make(chan []error)
	go func() {
		errs := make([]error, 0)
		for err := range ch {
			errs = append(errs, err)
		}
		allErrs <- errs
		close(allErrs)
	}()
	p := cache.ParamsWithDisposal{
		Name:   t.Name(),
		Shards: 3,
		Size:   size,
		OnDispose: func(v interface{}) error {
			element := v.(*record)
			if n := atomic.AddInt32(&element.disposed, 1); n != 1 {
				ch <- fmt.Errorf("%d disposals of %+v", n, *element)
			}
			if l := atomic.CompareAndSwapInt32(&element.live, 1, 0); !l {
				ch <- fmt.Errorf("disposal of already-dead %+v", *element)
			}
			return nil
		},
	}
	c := cache.NewCacheWithDisposal(p)

	numCreated := int32(0)

	wg := sync.WaitGroup{}
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func(i int) {
			for j := 0; j < repeats; j++ {
				k := j % size
				v, release, err := c.GetOrSet(
					k, func() (interface{}, error) {
						e := &record{
							key:      k,
							disposed: 0,
							live:     1,
						}
						elements[k] = e
						atomic.AddInt32(&numCreated, int32(1))
						return e, nil
					})
				if err != nil {
					ch <- err
				}
				e := v.(*record)
				if e.key != k {
					ch <- fmt.Errorf("got %v not %d", v, k)
				}
				if e.live == 0 {
					ch <- fmt.Errorf("got dead element %+v at %d", v, k)
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
	errs := <-allErrs
	for _, err := range errs {
		t.Error(err)
	}
}
