package catalog

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/go-multierror"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
	"google.golang.org/protobuf/proto"
)

const (
	ImportCanceled = "Canceled"
	statusChanSize = 100
)

var ErrImportClosed = errors.New("import closed")

type Import struct {
	db            *pebble.DB
	dbPath        string
	kvStore       kv.Store
	status        graveler.ImportStatus
	StatusChan    chan graveler.ImportStatus
	logger        logging.Logger
	repoPartition string
	progress      int64
	wg            multierror.Group
	closed        bool
}

func NewImport(ctx context.Context, cancel context.CancelFunc, logger logging.Logger, kvStore kv.Store, repository *graveler.RepositoryRecord, importID string) (*Import, error) {
	status := graveler.ImportStatus{
		ID:        graveler.ImportID(importID),
		UpdatedAt: time.Now(),
	}
	repoPartition := graveler.RepoPartition(repository)
	// Must be set first
	err := kv.SetMsg(ctx, kvStore, repoPartition, []byte(importID), graveler.ProtoFromImportStatus(&status))
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
		StatusChan:    make(chan graveler.ImportStatus, statusChanSize),
		logger:        logger,
		repoPartition: repoPartition,
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

func (i *Import) Set(record EntryRecord) error {
	if i.closed {
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
	if i.closed {
		return ErrImportClosed
	}
	i.logger.WithField("itr", it).Debug("Ingest start")
	for it.Next() {
		if err := i.Set(*it.Value()); err != nil {
			return err
		}
		i.progress += 1
	}
	i.logger.WithField("itr", it).Debug("Ingest finished")
	return nil
}

func (i *Import) Status() graveler.ImportStatus {
	return i.status
}

func (i *Import) NewItr() (*importIterator, error) {
	if i.closed {
		return nil, ErrImportClosed
	}
	return newImportIterator(i.db.NewIter(nil)), nil
}

func (i *Import) Close() {
	i.closed = true
	close(i.StatusChan)
	_ = i.wg.Wait().ErrorOrNil()
	_ = os.RemoveAll(i.dbPath)
}

func (i *Import) importStatusAsync(ctx context.Context, cancel context.CancelFunc) error {
	const statusUpdateInterval = 1 * time.Second
	statusData := graveler.ImportStatusData{}
	newStatus := i.status
	timer := time.NewTimer(statusUpdateInterval)
	done := false

	for {
		select {
		case s, ok := <-i.StatusChan:
			if ok {
				newStatus = s
			}
		case <-timer.C:
			pred, err := kv.GetMsg(ctx, i.kvStore, i.repoPartition, []byte(i.status.ID), &statusData)
			if err != nil {
				cancel()
				return err
			}
			if statusData.Error != "" || statusData.Completed {
				cancel()
				return nil
			}
			newStatus.UpdatedAt = time.Now()
			newStatus.Progress = i.progress
			// TODO: Remove key after 24 hrs
			err = kv.SetMsgIf(ctx, i.kvStore, i.repoPartition, []byte(i.status.ID), graveler.ProtoFromImportStatus(&newStatus), pred)
			if errors.Is(err, kv.ErrPredicateFailed) {
				i.logger.Warning("Import status update failed")
			} else if err != nil {
				cancel()
				return err
			}
			i.status = newStatus

			// Check if import closed, we want to do that only after we update the import state
			if done {
				return nil
			}
			timer.Reset(statusUpdateInterval)
		}
		if i.closed {
			done = true
		}
	}
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
