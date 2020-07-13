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

	go func() {
		acq := c.Lock("foo", func() {
			fmt.Printf("foo update called!\n")
			time.Sleep(time.Millisecond * 50)
			wg.Done()
		})

		if !acq {
			t.Fatalf("expected to acquire foo lock")
		}
	}()

	go func() {
		time.Sleep(10 * time.Millisecond)
		acq := c.Lock("foo", func() {
			t.Fatalf("foo shouldnt be called!\n")
		})
		if acq {
			t.Fatalf("foo shouldnt be called!\n")
		}
		wg.Done()
	}()

	go func() {
		time.Sleep(10 * time.Millisecond)
		acq := c.Lock("bar", func() {
			fmt.Printf("bar update called!\n")
			wg.Done()
		})
		if !acq {
			t.Fatalf("expected to acquire bar lock")
		}
	}()

	wg.Wait()
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
