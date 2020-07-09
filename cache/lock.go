package cache

import "sync"

type Locker interface {
	Lock(v interface{}, onAcquireFn func())
}

type ChanLocker struct {
	m *sync.Map
}

func NewChanLocker() *ChanLocker {
	return &ChanLocker{
		m: &sync.Map{},
	}
}

func (c ChanLocker) Lock(v interface{}, onAcquireFn func()) (acquired bool) {
	tid := make(chan struct{})
	actual, alreadyLocked := c.m.LoadOrStore(v, tid)
	if !alreadyLocked {
		onAcquireFn()
		c.m.Delete(v)
		close(tid)
		return true
	}

	<-actual.(chan struct{})
	return false
}
