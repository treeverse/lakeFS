package local

import (
	"context"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	LocalDriverName = "local"
	MemDriverName   = "mem"
	memPrefetchSize = 12
)

var (
	localDriverLock = &sync.Mutex{}
	dbMap           = make(map[string]*Store)
)

type LocalDriver struct{}

type MemDriver struct{}

func (d *LocalDriver) Open(ctx context.Context, kvParams kvparams.Config) (kv.Store, error) {
	params := kvParams.Local
	if params == nil {
		return nil, fmt.Errorf("missing %s settings: %w", LocalDriverName, kv.ErrDriverConfiguration)
	}

	localDriverLock.Lock()
	defer localDriverLock.Unlock()
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

func (d *MemDriver) Open(ctx context.Context, _ kvparams.Config) (kv.Store, error) {
	// no database open for this path
	logger := logging.FromContext(ctx).WithField("store", "mem")
	opts := badger.DefaultOptions("").WithInMemory(true)
	opts.Logger = &BadgerLogger{logger}
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	connection := &Store{
		db:           db,
		logger:       logger,
		prefetchSize: memPrefetchSize,
	}
	connection.refCount++
	return connection, nil
}

//nolint:gochecknoinits
func init() {
	kv.Register(LocalDriverName, &LocalDriver{})
	kv.Register(MemDriverName, &MemDriver{})
}
