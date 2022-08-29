package pyramid

import (
	"sync"

	"github.com/treeverse/lakefs/pkg/pyramid/params"
)

// fileLocker allows file locking to avoid race conditions between cache rejection/eviction
// and the file usage by TierFS
type fileLocker struct {
	wgMap *sync.Map
}

func newFileLocker() *fileLocker {
	return &fileLocker{
		wgMap: &sync.Map{},
	}
}

func (locker *fileLocker) lock(path params.RelativePath) (done func()) {
	fileWG := &sync.WaitGroup{}
	fileWG.Add(1)

	rawWG, loaded := locker.wgMap.LoadOrStore(path, fileWG)
	if loaded {
		// loaded - we can remove the recently added wg
		wg := rawWG.(*sync.WaitGroup)
		wg.Add(1)
		fileWG.Done()

		return wg.Done
	}

	return fileWG.Done
}

func (locker *fileLocker) waitUntilUnlock(path params.RelativePath) {
	rawWG, ok := locker.wgMap.Load(path)
	if ok {
		wg := rawWG.(*sync.WaitGroup)
		wg.Wait()
	}
}
