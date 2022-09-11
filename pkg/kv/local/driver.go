package local

import (
	"context"
	"github.com/dgraph-io/badger/v3"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/params"
	"github.com/treeverse/lakefs/pkg/logging"
	"sync"
)

const (
	DriverName          = "local"
	DefaultPrefetchSize = 256
)

var (
	driverLock    = &sync.Mutex{}
	connectionMap = make(map[string]*Store)
)

type Driver struct{}

func (d *Driver) Open(ctx context.Context, params params.KV) (kv.Store, error) {
	driverLock.Lock()
	defer driverLock.Unlock()
	connection, ok := connectionMap[params.Local.DirectoryPath]
	if !ok {
		// no database open for this path
		logger := logging.FromContext(ctx).WithField("store", "local")
		opts := badger.DefaultOptions(params.Local.DirectoryPath)
		opts.Logger = &BadgerLogger{logger}
		if params.Local.DisableLogging {
			logger = logging.DummyLogger{}
		}
		db, err := badger.Open(opts)
		if err != nil {
			return nil, err
		}
		prefetchSize := params.Local.PrefetchSize
		if prefetchSize == 0 {
			prefetchSize = DefaultPrefetchSize
		}
		connection = &Store{
			db:           db,
			logger:       logger,
			prefetchSize: prefetchSize,
			path:         params.Local.DirectoryPath,
		}
		connectionMap[params.Local.DirectoryPath] = connection
	}
	connection.refCount++
	return connection, nil
}

//nolint:gochecknoinits
func init() {
	kv.Register(DriverName, &Driver{})
}
