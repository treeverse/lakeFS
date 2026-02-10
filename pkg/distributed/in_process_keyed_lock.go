package distributed

import (
	"container/list"
	"context"
	"sync"
)

// InProcessKeyedLock serializes access per key inside a single process
// with strict FIFO ordering and context-aware cancellation.
//
// Waiters for the same key are served in the order they called Acquire.
// Any waiter may bail early if its context is cancelled; in that case the
// next waiter in line is promoted without an extra goroutine.
//
// Usage:
//   - Create once (for example, as a field on a long-lived manager).
//   - Call Acquire(ctx, key) before entering a key-specific critical section.
//   - Always call the returned release function when done.
type InProcessKeyedLock struct {
	mu   sync.Mutex
	keys map[string]*keyQueue
}

// keyQueue tracks the current holder and ordered waiters for a single key.
type keyQueue struct {
	held    bool
	waiters list.List // elements are *waiter
}

// waiter is a slot in the FIFO queue.  Its channel is closed exactly once
// to signal that it is now the waiter's turn.
type waiter struct {
	ch chan struct{}
}

func NewInProcessKeyedLock() *InProcessKeyedLock {
	return &InProcessKeyedLock{
		keys: make(map[string]*keyQueue),
	}
}

// Acquire waits in FIFO order until key is available, or returns ctx.Err()
// if waiting was cancelled.  On success it returns a release function that
// must be called exactly once when the caller is done with the key.
func (l *InProcessKeyedLock) Acquire(ctx context.Context, key string) (func(), error) {
	l.mu.Lock()

	kq := l.getOrCreateLocked(key)

	if !kq.held {
		// Fast path: no contention.
		kq.held = true
		l.mu.Unlock()
		return l.makeRelease(key), nil
	}

	// Slow path: key is held — enqueue ourselves and wait.
	w := &waiter{ch: make(chan struct{})}
	elem := kq.waiters.PushBack(w)
	l.mu.Unlock()

	select {
	case <-w.ch:
		// We were signaled — we now hold the lock.
		return l.makeRelease(key), nil

	case <-ctx.Done():
		l.mu.Lock()
		// Between selecting ctx.Done() and acquiring mu, the holder
		// may have closed w.ch (handed off to us).  Check for that.
		select {
		case <-w.ch:
			// We received the handoff despite cancellation.
			// Pass it along to the next waiter in line.
			l.handoffLocked(key)
		default:
			// We were not signaled yet — remove ourselves.
			kq.waiters.Remove(elem)
			l.cleanupLocked(key)
		}
		l.mu.Unlock()
		return nil, ctx.Err()
	}
}

// makeRelease returns a release function for the given key.
// The caller must call the returned function exactly once.
func (l *InProcessKeyedLock) makeRelease(key string) func() {
	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.handoffLocked(key)
	}
}

// handoffLocked passes the lock to the next FIFO waiter, or marks the key
// as free if there are none.  Must be called with l.mu held.
func (l *InProcessKeyedLock) handoffLocked(key string) {
	kq := l.keys[key]
	for kq.waiters.Len() > 0 {
		front := kq.waiters.Front()
		kq.waiters.Remove(front)
		w := front.Value.(*waiter)
		close(w.ch)
		// The signaled waiter now holds the lock.
		return
	}
	// No waiters — release the key.
	kq.held = false
	l.cleanupLocked(key)
}

// getOrCreateLocked returns the keyQueue for key, creating it if absent.
// Must be called with l.mu held.
func (l *InProcessKeyedLock) getOrCreateLocked(key string) *keyQueue {
	kq, ok := l.keys[key]
	if !ok {
		kq = &keyQueue{}
		l.keys[key] = kq
	}
	return kq
}

// cleanupLocked removes the keyQueue entry if it is empty and not held.
// Must be called with l.mu held.
func (l *InProcessKeyedLock) cleanupLocked(key string) {
	kq, ok := l.keys[key]
	if !ok {
		return
	}
	if !kq.held && kq.waiters.Len() == 0 {
		delete(l.keys, key)
	}
}
