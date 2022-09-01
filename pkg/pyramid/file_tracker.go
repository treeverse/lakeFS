package pyramid

import (
	"sync"

	"github.com/treeverse/lakefs/pkg/pyramid/params"
)

// fileTracker tracks file open requests in TierFS to avoid race conditions with cache rejection/eviction
type fileTracker struct {
	refMap map[string]int
	mu     sync.Mutex
}

type decCallback func(int)

func newFileTracker() *fileTracker {
	return &fileTracker{
		refMap: map[string]int{},
		mu:     sync.Mutex{},
	}
}

func (t *fileTracker) inc(path params.RelativePath) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ref := 1

	if val, ok := t.refMap[string(path)]; ok {
		ref = val + 1
		t.refMap[string(path)] = ref
	} else {
		t.refMap[string(path)] = ref
	}
}

func (t *fileTracker) dec(path params.RelativePath, fn decCallback) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ref := 0

	if val, ok := t.refMap[string(path)]; ok {
		ref = val - 1
		t.refMap[string(path)] = ref
	}
	if ref == -1 {
		delete(t.refMap, string(path))
	}
	fn(ref)
}
