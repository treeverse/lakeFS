package cache_test

import (
	"fmt"
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
	go func(acq *bool, getter *bool) {
		*acq = c.Lock("foo", func() {
			time.Sleep(time.Millisecond * 100)
			*getter = true
			wg.Done()
		})
	}(&foo100, &getFoo100)

	var foo10, getFoo10 bool
	go func(acq *bool, getter *bool) {
		time.Sleep(10 * time.Millisecond)
		*acq = c.Lock("foo", func() {
			*getter = true
		})
		wg.Done()
	}(&foo10, &getFoo10)

	var bar10 bool
	var getBar10 bool
	go func(acq *bool, getter *bool) {
		time.Sleep(10 * time.Millisecond)
		*acq = c.Lock("bar", func() {
			*getter = true
			wg.Done()
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

func TestNewLRUCache(t *testing.T) {
	m := make(chan struct{})

	go func() {
		fmt.Printf("gonna read from m...\n")
		v := <-m
		fmt.Printf("read fron m! %v\n", v)
	}()

	time.Sleep(3 * time.Second)
	fmt.Printf("done sleeping! closing")
	close(m)
}
