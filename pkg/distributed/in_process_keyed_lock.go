package distributed

import (
	"context"
	"sync"
)

// InProcessKeyedLock serializes access per key inside a single process.
//
// Usage:
//   - Create once (for example, as a field on a long-lived manager).
//   - Call Acquire(ctx, key) before entering a key-specific critical section.
//   - Always call the returned release function (typically via defer).
//
// Acquire is context-aware: callers waiting on a busy key return immediately
// when ctx is cancelled.
type InProcessKeyedLock struct {
	mu    sync.Mutex
	locks map[string]*inProcessLock
}

type inProcessLock struct {
	ch   chan struct{}
	refs int
}

func NewInProcessKeyedLock() *InProcessKeyedLock {
	return &InProcessKeyedLock{
		locks: make(map[string]*inProcessLock),
	}
}

// Acquire waits until key is available, or returns ctx.Err() if waiting was
// cancelled. If successful it returns a release function is idempotent and must be called
// after successful Acquire.
func (l *InProcessKeyedLock) Acquire(ctx context.Context, key string) (func(), error) {
	lock := l.ref(key)
	select {
	case <-lock.ch:
	case <-ctx.Done():
		l.unref(key)
		return nil, ctx.Err()
	}

	return func() {
		lock.ch <- struct{}{}
		l.unref(key)
	}, nil
}

func (l *InProcessKeyedLock) ref(key string) *inProcessLock {
	l.mu.Lock()
	defer l.mu.Unlock()

	lock, ok := l.locks[key]
	if !ok {
		lock = &inProcessLock{
			ch: make(chan struct{}, 1),
		}
		lock.ch <- struct{}{}
		l.locks[key] = lock
	}
	lock.refs++
	return lock
}

func (l *InProcessKeyedLock) unref(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lock, ok := l.locks[key]
	if !ok {
		return
	}
	lock.refs--
	if lock.refs == 0 {
		delete(l.locks, key)
	}
}
