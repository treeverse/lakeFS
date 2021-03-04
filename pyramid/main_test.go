package pyramid

import (
	"context"
	"io"
	"os"
	"path"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/block/mem"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/pyramid/params"
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

func (a *memAdapter) Get(ctx context.Context, obj block.ObjectPointer, size int64) (io.ReadCloser, error) {
	atomic.AddInt64(&a.gets, 1)
	<-a.wait
	return a.Adapter.Get(ctx, obj, size)
}

func createFSWithEviction(ev params.Eviction) (FS, string) {
	fsName := uuid.New().String()
	baseDir := path.Join(os.TempDir(), fsName)

	// starting adapter with closed channel so all Gets pass
	adapter = &memAdapter{Adapter: mem.New(), wait: make(chan struct{})}
	close(adapter.wait)

	var err error
	fs, err = NewFS(&params.InstanceParams{
		FSName:              fsName,
		DiskAllocProportion: 1.0,
		SharedParams: params.SharedParams{
			Adapter:            adapter,
			Logger:             logging.Dummy(),
			Eviction:           ev,
			BlockStoragePrefix: blockStoragePrefix,
			Local: params.LocalDiskParams{
				BaseDir:             baseDir,
				TotalAllocatedBytes: allocatedDiskBytes,
			},
		},
	})
	if err != nil {
		panic(err)
	}

	return fs, baseDir
}

func TestMain(m *testing.M) {
	var baseDir string
	fs, baseDir = createFSWithEviction(nil)

	code := m.Run()

	// cleanup
	_ = os.RemoveAll(baseDir)

	os.Exit(code)
}
