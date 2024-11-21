package cache_test

import (
	"sync"
	"testing"
	"time"

	"github.com/treeverse/lakefs/modules/cache"
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
		if err != nil {
			t.Fatal("GetOrSet")
		}
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

// Spy manages a setFn that always returns a constant and spies on whether
// it was called.
type Spy struct {
	value  interface{}
	expiry time.Duration
	called bool
}

func NewSpy(value interface{}, expiry time.Duration) *Spy {
	return &Spy{
		value:  value,
		expiry: expiry,
		called: false,
	}
}

// MakeSetFn returns a SetFn that remembers it was called and returns the
// configured value.
func (s *Spy) MakeSetFn() cache.SetFnWithExpiry {
	return cache.SetFnWithExpiry(func() (interface{}, time.Duration, error) {
		s.called = true
		return s.value, s.expiry, nil
	})
}

// ResetCalled forgets whether the SetFn was called.
func (s *Spy) ResetCalled() {
	s.called = false
}

// Called returns true if the SetFn was called.
func (s *Spy) Called() bool {
	return s.called
}

func TestCacheGetSetWithExpiry(t *testing.T) {
	const (
		cacheSize = 100
		value     = 17
		// fastExpiry should be long enough for some code to
		// execute, but short enough to allow waiting for it a few
		// times.
		fastExpiry = time.Millisecond * 5
		// slowExpiry should not expire before the test is done.
		slowExpiry = time.Hour * 5
	)

	c := cache.NewCache(cacheSize, time.Millisecond, cache.NewJitterFn(time.Millisecond))

	fastSpy := NewSpy(value, fastExpiry)
	fastSetFn := fastSpy.MakeSetFn()
	slowSpy := NewSpy(value, slowExpiry)
	slowSetFn := slowSpy.MakeSetFn()

	_, err := c.GetOrSetWithExpiry("long-lived", slowSetFn)
	if err != nil {
		t.Errorf("Failed to GetSet long-lived: %s", err)
	}
	if !slowSpy.Called() {
		t.Error("Expected to call SetFn for long-lived")
	}
	slowSpy.ResetCalled()

	_, err = c.GetOrSetWithExpiry("short-lived", fastSetFn)
	if err != nil {
		t.Errorf("Failed to GetSet short-lived: %s", err)
	}
	if !fastSpy.Called() {
		t.Error("Expected to call SetFn for short-lived")
	}
	fastSpy.ResetCalled()

	_, err = c.GetOrSetWithExpiry("short-lived", fastSetFn)
	if err != nil {
		t.Errorf("Failed to GetSet short-lived: %s", err)
	}
	if fastSpy.Called() {
		t.Error("Expected not to call SetFn for short-lived so soon after")
	}
	fastSpy.ResetCalled()

	time.Sleep(2 * fastExpiry)
	_, err = c.GetOrSetWithExpiry("short-lived", fastSetFn)
	if err != nil {
		t.Errorf("Failed to GetSet short-lived: %s", err)
	}
	if !fastSpy.Called() {
		t.Error("Expected to call SetFn after expiry for short-lived")
	}
	fastSpy.ResetCalled()

	_, err = c.GetOrSetWithExpiry("long-lived", slowSetFn)
	if err != nil {
		t.Errorf("Failed to GetSet long-lived: %s", err)
	}
	if slowSpy.Called() {
		t.Error("Expected not to call SetFn again for long-lived")
	}
	slowSpy.ResetCalled()
}
