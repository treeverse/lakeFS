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
	DriverName = "local"
)

var (
	driverLock = &sync.Mutex{}
	dbMap      = make(map[string]*Store)
)

type Driver struct{}

func (d *Driver) Open(ctx context.Context, kvParams kvparams.Config) (kv.Store, error) {
	params := kvParams.Local
	if params == nil {
		return nil, fmt.Errorf("missing %s settings: %w", DriverName, kv.ErrDriverConfiguration)
	}

	driverLock.Lock()
	defer driverLock.Unlock()
	connection, ok := dbMap[params.Path]
	if !ok {
		// no database open for this path
		var logger logging.Logger = logging.DummyLogger{}
		if params.EnableLogging {
			logger = logging.FromContext(ctx).WithField("store", "local")
		}
		opts := badger.DefaultOptions(params.Path)
		opts.Logger = &BadgerLogger{logger}
		db, err := badger.Open(opts)
		if err != nil {
			return nil, err
		}
		connection = &Store{
			db:           db,
			logger:       logger,
			prefetchSize: params.PrefetchSize,
			path:         params.Path,
		}
		dbMap[params.Path] = connection
	}
	connection.refCount++
	return connection, nil
}

//nolint:gochecknoinits
func init() {
	kv.Register(DriverName, &Driver{})
}
