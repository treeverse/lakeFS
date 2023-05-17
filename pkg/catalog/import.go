package catalog

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/go-multierror"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
	"google.golang.org/protobuf/proto"
)

const ImportCanceled = "Canceled"

var ErrImportClosed = errors.New("import closed")

type Import struct {
	db            *pebble.DB
	dbPath        string
	kvStore       kv.Store
	logger        logging.Logger
	repoPartition string
	wg            multierror.Group
	status        graveler.ImportStatus
	closed        bool
	mu            sync.Mutex
}

func NewImport(ctx context.Context, cancel context.CancelFunc, logger logging.Logger, kvStore kv.Store, repository *graveler.RepositoryRecord, importID, importBranch string) (*Import, error) {
	status := graveler.ImportStatus{
		ID:           graveler.ImportID(importID),
		UpdatedAt:    time.Now(),
		ImportBranch: importBranch,
	}
	repoPartition := graveler.RepoPartition(repository)
	// Must be set first
	err := kv.SetMsg(ctx, kvStore, repoPartition, []byte(graveler.ImportsPath(importID)), graveler.ProtoFromImportStatus(&status))
	if err != nil {
		return nil, err
	}
	dbPath, err := os.MkdirTemp("", "import_"+importID)
	if err != nil {
		return nil, err
	}
	importDB, err := pebble.Open(dbPath, nil)
	if err != nil {
		return nil, err
	}

	i := Import{
		db:            importDB,
		dbPath:        dbPath,
		kvStore:       kvStore,
		status:        status,
		logger:        logger,
		repoPartition: repoPartition,
		mu:            sync.Mutex{},
	}

	i.wg.Go(func() error {
		err := i.importStatusAsync(ctx, cancel)
		if err != nil {
			i.logger.WithError(err).Error("failed to update import status")
		}
		return err
	})
	return &i, nil
}

func (i *Import) set(record EntryRecord) error {
	if i.Closed() {
		return ErrImportClosed
	}
	key := []byte(record.Path)
	value, err := EntryToValue(record.Entry)
	if err != nil {
		return err
	}
	pb := graveler.ProtoFromStagedEntry(key, value)
	data, err := proto.Marshal(pb)
	if err != nil {
		return err
	}
	return i.db.Set(key, data, &pebble.WriteOptions{
		Sync: false,
	})
}

func (i *Import) Ingest(it *walkEntryIterator) error {
	if i.Closed() {
		return ErrImportClosed
	}

	i.logger.WithField("itr", it).Debug("Ingest start")
	for it.Next() {
		if err := i.set(*it.Value()); err != nil {
			return err
		}
		i.mu.Lock()
		i.status.Progress += 1
		i.mu.Unlock()
	}
	i.logger.WithField("itr", it).Debug("Ingest finished")
	return nil
}

func (i *Import) Status() graveler.ImportStatus {
	i.mu.Lock()
	status := i.status
	i.mu.Unlock()
	return status
}

func (i *Import) SetStatus(status graveler.ImportStatus) {
	i.mu.Lock()
	i.status = status
	i.status.UpdatedAt = time.Now()
	i.mu.Unlock()
}

func (i *Import) SetError(err error) {
	i.mu.Lock()
	i.status.Error = err
	i.status.UpdatedAt = time.Now()
	i.mu.Unlock()
}

func (i *Import) NewItr() (*importIterator, error) {
	if i.closed {
		return nil, ErrImportClosed
	}
	return newImportIterator(i.db.NewIter(nil)), nil
}

func (i *Import) Closed() bool {
	i.mu.Lock()
	closed := i.closed
	i.mu.Unlock()
	return closed
}

func (i *Import) Close() {
	i.mu.Lock()
	i.closed = true
	i.mu.Unlock()
	_ = i.wg.Wait().ErrorOrNil()
	_ = i.db.Close()
	_ = os.RemoveAll(i.dbPath)
}

func (i *Import) importStatusAsync(ctx context.Context, cancel context.CancelFunc) error {
	const statusUpdateInterval = 1 * time.Second
	statusData := graveler.ImportStatusData{}
	ticker := time.NewTicker(statusUpdateInterval)
	defer ticker.Stop()
	done := false

	for range ticker.C {
		pred, err := kv.GetMsg(ctx, i.kvStore, i.repoPartition, []byte(graveler.ImportsPath(i.status.ID.String())), &statusData)
		if err != nil {
			cancel()
			return err
		}
		if statusData.Error != "" || statusData.Completed {
			cancel()
			return nil
		}
		currStatus := i.Status()
		currStatus.UpdatedAt = time.Now()
		err = kv.SetMsgIf(ctx, i.kvStore, i.repoPartition, []byte(graveler.ImportsPath(i.status.ID.String())), graveler.ProtoFromImportStatus(&currStatus), pred)
		if errors.Is(err, kv.ErrPredicateFailed) {
			i.logger.Warning("Import status update failed")
		} else if err != nil {
			cancel()
			return err
		}

		// Ensure one last status update after import closed
		if done {
			return nil
		}
		if i.Closed() {
			done = true
		}
	}
	return nil
}

type importIterator struct {
	it      *pebble.Iterator
	hasMore bool
	value   *graveler.ValueRecord
	err     error
}

func newImportIterator(it *pebble.Iterator) *importIterator {
	itr := &importIterator{
		it: it,
	}
	itr.hasMore = it.First()
	return itr
}

func (it *importIterator) Next() bool {
	if it.err != nil {
		return false
	}
	if !it.hasMore {
		it.value = nil
		return false
	}
	var value []byte
	value, it.err = it.it.ValueAndErr()
	if it.err != nil {
		return false
	}
	data := graveler.StagedEntryData{}
	it.err = proto.Unmarshal(value, &data)
	if it.err != nil {
		return false
	}
	it.value = &graveler.ValueRecord{
		Key:   data.Key,
		Value: graveler.StagedEntryFromProto(&data),
	}
	it.hasMore = it.it.Next()
	return true
}

func (it *importIterator) SeekGE(id graveler.Key) {
	it.value = nil
	it.hasMore = it.it.SeekGE(id)
}

func (it *importIterator) Value() *graveler.ValueRecord {
	return it.value
}

func (it *importIterator) Err() error {
	return it.err
}

func (it *importIterator) Close() {
	_ = it.it.Close()
}
