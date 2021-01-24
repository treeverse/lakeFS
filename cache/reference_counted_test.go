package cache_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/treeverse/lakefs/cache"
	"github.com/treeverse/lakefs/logging"
)

func TestCacheWithDisposal(t *testing.T) {
	type record struct {
		key      int
		disposed int32
		live     int32
	}
	ctx := context.Background()

	cases := []struct {
		name                       string
		size, parallelism, repeats int
		delay                      time.Duration
	}{
		{name: "full speed", size: 7, parallelism: 100, repeats: 2000},
		{name: "try to race", size: 7, parallelism: 9, repeats: 500, delay: time.Millisecond},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var elements = make([]*record, tc.size)
			ch := make(chan error, tc.parallelism*2)
			allErrs := make(chan []error, tc.parallelism*2)
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
				Logger: logging.Default().WithField("testing", true),
				Shards: 3,
				Size:   tc.size,
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
			for i := 0; i < tc.parallelism; i++ {
				wg.Add(1)
				go func(i int) {
					for j := 0; j < tc.repeats; j++ {
						k := j % tc.size
						v, release, err := c.GetOrSet(
							ctx,
							k, func() (interface{}, error) {
								if tc.delay > 0 {
									time.Sleep(tc.delay)
								}
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
						if (j*tc.repeats+i)%113 == 17 {
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
		})
	}
}
