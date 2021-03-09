package cache_test

import (
	"sync"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/cache"
	"github.com/treeverse/lakefs/pkg/testutil"
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
