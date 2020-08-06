package cache_test

import (
	"sync"
	"testing"
	"time"

	"github.com/treeverse/lakefs/cache"
)

func TestChanLocker_LockAfterLock(t *testing.T) {
	c := cache.NewChanLocker()
	acq := c.Lock("foo", func() {})
	if !acq {
		t.Fatalf("expected first lock to acquire")
	}

	acq = c.Lock("foo", func() {})
	if !acq {
		t.Fatalf("expected second lock to acquire")
	}
}

func TestChanLocker_Lock(t *testing.T) {
	c := cache.NewChanLocker()

	var wg sync.WaitGroup
	wg.Add(3)

	var foo100, getFoo100 bool
	ch := make(chan bool)
	go func(acq *bool, getter *bool, ch chan bool) {
		close(ch)
		defer wg.Done()
		*acq = c.Lock("foo", func() {
			*getter = true
			time.Sleep(time.Millisecond * 100)
		})
	}(&foo100, &getFoo100, ch)
	<-ch // wait until goroutine starts

	var foo10, getFoo10 bool
	go func(acq *bool, getter *bool) {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		*acq = c.Lock("foo", func() {
			*getter = true
		})
	}(&foo10, &getFoo10)

	var bar10 bool
	var getBar10 bool
	go func(acq *bool, getter *bool) {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		*acq = c.Lock("bar", func() {
			*getter = true
		})
	}(&bar10, &getBar10)

	wg.Wait()
	if foo10 {
		t.Error("expected to not acquire foo after 10ms")
	}
	if getFoo10 {
		t.Error("expected foo (10ms) getter not to be called")
	}
	if !getFoo100 {
		t.Error("expected foo (100ms) getter to be called")
	}
	if !foo100 {
		t.Error("expected to acquire foo after 100ms")
	}
	if !getBar10 {
		t.Error("expected bar getter to be called")
	}
	if !bar10 {
		t.Error("expected to acquire bar")
	}
}
