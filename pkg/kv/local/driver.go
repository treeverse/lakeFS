package local

import (
	"context"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v3"
	"github.com/treeverse/lakefs/pkg/kv"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	DriverName           = "local"
	DefaultDirectoryPath = "~/data/lakefs/kv"
	DefaultPrefetchSize  = 256
)

var (
	driverLock    = &sync.Mutex{}
	connectionMap = make(map[string]*Store)
)

type Driver struct{}

func normalizeDBParams(p *kvparams.Local) {
	if len(p.DirectoryPath) == 0 {
		p.DirectoryPath = DefaultDirectoryPath
	}
	if p.PrefetchSize <= 0 {
		p.PrefetchSize = DefaultPrefetchSize
	}
}

func (d *Driver) Open(ctx context.Context, kvParams kvparams.KV) (kv.Store, error) {
	driverLock.Lock()
	defer driverLock.Unlock()
	params := kvParams.Local
	if params == nil {
		return nil, fmt.Errorf("missing %s settings: %w", DriverName, kv.ErrDriverConfiguration)
	}
	normalizeDBParams(params)
	connection, ok := connectionMap[params.DirectoryPath]
	if !ok {
		// no database open for this path
		var logger logging.Logger = logging.DummyLogger{}
		if params.EnableLogging {
			logger = logging.FromContext(ctx).WithField("store", "local")
		}
		opts := badger.DefaultOptions(params.DirectoryPath)
		opts.Logger = &BadgerLogger{logger}
		db, err := badger.Open(opts)
		if err != nil {
			return nil, err
		}
		prefetchSize := params.PrefetchSize
		if prefetchSize == 0 {
			prefetchSize = DefaultPrefetchSize
		}
		connection = &Store{
			db:           db,
			logger:       logger,
			prefetchSize: prefetchSize,
			path:         params.DirectoryPath,
		}
		connectionMap[params.DirectoryPath] = connection
	}
	connection.refCount++
	return connection, nil
}

//nolint:gochecknoinits
func init() {
	kv.Register(DriverName, &Driver{})
}
