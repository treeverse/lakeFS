package pyramid

import (
	"sync"
	"time"
)

type accessManager struct {
	sync.Mutex

	refCount   map[string]int
	lastAccess map[string]time.Time
}

func newAccessManager() *accessManager {
	return &accessManager{
		refCount:   map[string]int{},
		lastAccess: map[string]time.Time{},
	}
}

func (am *accessManager) addRef(filename string) {
	am.Lock()
	defer am.Unlock()

	am.refCount[filename]++
}

func (am *accessManager) touch(filename string) {
	am.Lock()
	defer am.Unlock()

	am.lastAccess[filename] = time.Now()
}

func (am *accessManager) removeRef(filename string) {
	am.Lock()
	defer am.Unlock()

	am.refCount[filename]--
	if am.refCount[filename] == 0 {
		delete(am.refCount, filename)
	}
}
