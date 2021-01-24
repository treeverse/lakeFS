package cache_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/treeverse/lakefs/cache"
	"github.com/treeverse/lakefs/testutil"
)

func TestOnlyOne_ComputeInSequence(t *testing.T) {
	const (
		one = "foo"
		two = "bar"
	)
	c := cache.NewChanOnlyOne()
	first, err := c.Compute("foo", func() (interface{}, error) { return one, nil })
	testutil.MustDo(t, "first Compute", err)
	second, err := c.Compute("foo", func() (interface{}, error) { return two, nil })
	testutil.MustDo(t, "second Compute", err)
	if first.(string) != one {
		t.Errorf("got first compute %s, expected %s", first, one)
	}
	if second.(string) != two {
		t.Errorf("got second compute %s, expected %s", second, two)
	}
}

func TestOnlyOne_ComputeConcurrentlyOnce(t *testing.T) {
	c := cache.NewChanOnlyOne()

	var wg sync.WaitGroup
	wg.Add(3)

	ch := make(chan struct{})
	did100 := false
	go func(didIt *bool) {
		defer wg.Done()
		value, err := c.Compute("foo", func() (interface{}, error) {
			close(ch)
			*didIt = true
			time.Sleep(time.Millisecond * 100)
			return 100, nil
		})
		if value != 100 || err != nil {
			t.Errorf("got %v, %v not 100, nil", value, err)
		}
	}(&did100)

	<-ch // Ensure first computation is in progress

	did10 := false
	go func(didIt *bool) {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		value, err := c.Compute("foo", func() (interface{}, error) {
			*didIt = true
			return 101, nil
		})
		if value != 100 || err != nil {
			t.Errorf("got %v, %v not 100, nil", value, err)
		}
	}(&did10)

	did5 := false
	go func(didIt *bool) {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		value, err := c.Compute("foo", func() (interface{}, error) {
			*didIt = true
			return 102, nil
		})
		if value != 100 || err != nil {
			t.Errorf("got %v, %v not 100, nil", value, err)
		}
	}(&did5)

	wg.Wait()
	if !did100 {
		t.Error("expected to run first concurrent compute and wait 100ms")
	}
	if did10 {
		t.Error("did not expect to run concurrent compute after 10ms")
	}
	if did5 {
		t.Error("did not expect to run concurrent compute after 5ms")
	}
}

func TestOnlyOne_ComputeConcurrentlyOnceKeyedByStruct(t *testing.T) {
	type Key struct {
		a, b int
	}
	const (
		size        = 3
		repeats     = 5
		parallelism = 9
	)

	for i := 0; i < repeats; i++ {
		c := cache.NewChanOnlyOne()
		t.Run(fmt.Sprintf("%03d", i), func(t *testing.T) {
			var wg sync.WaitGroup
			ch := make(chan struct{})
			numExec := int32(0)
			wg.Add(parallelism)
			for j := 0; j < parallelism; j++ {
				go func() {
					<-ch
					for k := 0; k < size; k++ {
						for l := 0; l < size; l++ {
							v, err := c.Compute(Key{k, l}, func() (interface{}, error) {
								atomic.AddInt32(&numExec, 1)
								return Key{k + 1, l + 1}, nil
							})
							if err != nil {
								t.Error(err)
								continue
							}
							got := v.(Key)
							expected := Key{a: k + 1, b: l + 1}
							if got != expected {
								t.Errorf("got %v instead of [%d %d]", got, k+1, l+1)
							}
						}
					}
					wg.Done()
				}()
			}
			close(ch)
			wg.Wait()
			if numExec != size*size {
				t.Errorf("Computed callback %d times not %d", numExec, size*size)
			}
		})
	}
}
