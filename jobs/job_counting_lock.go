package jobs

import (
	"sync"
)

type CountingLock struct {
	cond       *sync.Cond
	counter    int64
	maxCounter int64
}
type CountingLockRef struct {
	c      *CountingLock
	isHeld bool
}

func NewCountingLock(max int64) *CountingLock {
	lock := new(sync.Mutex)
	return &CountingLock{cond: sync.NewCond(lock),
		counter:    max,
		maxCounter: max}
}

func (c *CountingLock) NewCountingLockRef() *CountingLockRef {
	return &CountingLockRef{c: c,
		isHeld: false}
}

func (cr *CountingLockRef) GetAccess() {
	if cr.isHeld {
		panic("trying to take lock that is already held")
	}
	lock := cr.c.cond.L
	lock.Lock()
	if cr.c.counter < 0 {
		panic("CountingLock negative OUTside wait loop")
	}
	for cr.c.counter == 0 {
		cr.c.cond.Wait()
	}
	if cr.c.counter <= 0 {
		panic("CountingLock negative INside wait loop")
	}
	cr.c.counter--
	cr.isHeld = true
	lock.Unlock()
}

func (cr *CountingLockRef) Release(deferred bool) {
	lock := cr.c.cond.L
	lock.Lock()
	if !cr.isHeld {
		if deferred {
			lock.Unlock()
			return
		} else {
			panic("releasing unheld counting lock")
		}
	}
	if cr.c.counter >= cr.c.maxCounter {
		panic("counting lock counter goes above maximum")
	}
	cr.c.counter++
	cr.isHeld = false
	lock.Unlock()
	cr.c.cond.Signal()
}
