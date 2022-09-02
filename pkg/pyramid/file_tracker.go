package pyramid

import (
	"sync"

	"github.com/treeverse/lakefs/pkg/pyramid/params"
)

// fileTracker tracks file open requests in TierFS to avoid race conditions with cache rejection/eviction
type fileTracker struct {
	refMap map[string]*tracked
	mu     sync.Mutex
	delete deleteCallback
}

type deleteCallback func(path params.RelativePath)

type tracked struct {
	ref     int
	deleted bool
}

func NewFileTracker(delete deleteCallback) *fileTracker {
	return &fileTracker{
		refMap: map[string]*tracked{},
		delete: delete,
	}
}

func (t *fileTracker) Open(path params.RelativePath) func() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if val, ok := t.refMap[string(path)]; ok {
		val.ref++
	} else {
		t.refMap[string(path)] = &tracked{
			ref: 1,
		}
	}
	return func() {
		t.close(path)
	}
}

func (t *fileTracker) close(path params.RelativePath) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if val, ok := t.refMap[string(path)]; ok {
		val.ref--
		if val.ref == 0 {
			delete(t.refMap, string(path))
			if val.deleted {
				t.delete(path)
			}
		}
	}
}

func (t *fileTracker) Delete(path params.RelativePath) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if val, ok := t.refMap[string(path)]; ok {
		val.deleted = true
	} else {
		t.delete(path)
	}
}
