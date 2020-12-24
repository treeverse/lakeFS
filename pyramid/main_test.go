package pyramid

import (
	"io"
	"os"
	"path"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/block/mem"
	"github.com/treeverse/lakefs/logging"
)

// memAdapter a memory based Adapter that count gets and let you wait
type memAdapter struct {
	gets int64
	wait chan struct{}
	*mem.Adapter
}

var (
	// global fs used
	fs FS

	// global adapter used by fs
	adapter *memAdapter
)

func (a *memAdapter) GetCount() int64 {
	return atomic.LoadInt64(&a.gets)
}

func (a *memAdapter) Get(obj block.ObjectPointer, size int64) (io.ReadCloser, error) {
	atomic.AddInt64(&a.gets, 1)
	<-a.wait
	return a.Adapter.Get(obj, size)
}

func TestMain(m *testing.M) {
	fsName := uuid.New().String()
	baseDir := path.Join(os.TempDir(), fsName)

	// starting adapter with closed channel so all Gets pass
	adapter = &memAdapter{Adapter: mem.New(), wait: make(chan struct{})}
	close(adapter.wait)

	var err error
	fs, err = NewFS(&Config{
		fsName:               fsName,
		adaptor:              adapter,
		logger:               logging.Dummy(),
		fsBlockStoragePrefix: blockStoragePrefix,
		localBaseDir:         baseDir,
		allocatedDiskBytes:   allocatedDiskBytes,
	})
	if err != nil {
		panic(err)
	}

	code := m.Run()

	// cleanup
	_ = os.RemoveAll(baseDir)

	os.Exit(code)
}
