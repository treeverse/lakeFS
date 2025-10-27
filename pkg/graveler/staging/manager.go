package staging

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/batch"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

type Manager struct {
	kvStore                     kv.Store
	kvStoreLimited              kv.Store
	wakeup                      chan asyncEvent
	batchExecutor               batch.Batcher
	batchDBIOTransactionMarkers bool

	// cleanupCallback is being called with every successful cleanup cycle
	cleanupCallback func()
}

// asyncEvent is a type of event to be handled in the async loop
type asyncEvent string

// cleanTokens is async cleaning of deleted staging tokens
const (
	MaxBatchDelay = 3 * time.Millisecond
	cleanTokens   = asyncEvent("clean_tokens")
)

func NewManager(ctx context.Context, store, storeLimited kv.Store, batchDBIOTransactionMarkers bool, executor batch.Batcher) *Manager {
	const wakeupChanCapacity = 100
	m := &Manager{
		kvStore:                     store,
		kvStoreLimited:              storeLimited,
		wakeup:                      make(chan asyncEvent, wakeupChanCapacity),
		batchExecutor:               executor,
		batchDBIOTransactionMarkers: batchDBIOTransactionMarkers,
	}
	go m.asyncLoop(ctx)
	return m
}

func (m *Manager) log(ctx context.Context) logging.Logger {
	return logging.FromContext(ctx).WithField("service_name", "staging_manager")
}

func (m *Manager) OnCleanup(cleanupCallback func()) {
	m.cleanupCallback = cleanupCallback
}

func (m *Manager) getBatchedEntryData(ctx context.Context, st graveler.StagingToken, key graveler.Key) (*graveler.StagedEntryData, error) {
	batchKey := fmt.Sprintf("StagingGet:%s:%s", st, key)
	dt, err := m.batchExecutor.BatchFor(ctx, batchKey, MaxBatchDelay, batch.ExecuterFunc(func() (interface{}, error) {
		dt := &graveler.StagedEntryData{}
		_, err := kv.GetMsg(ctx, m.kvStore, graveler.StagingTokenPartition(st), key, dt)
		return dt, err
	}))
	if err != nil {
		return nil, err
	}
	return dt.(*graveler.StagedEntryData), nil
}

// isDBIOTransactionalMarkerObject returns true if the key is a DBIO transactional marker object (_started or _committed
func isDBIOTransactionalMarkerObject(key graveler.Key) bool {
	_, f := path.Split(key.String())
	return strings.HasPrefix(f, "_started_") || strings.HasPrefix(f, "_committed_")
}

func (m *Manager) Get(ctx context.Context, st graveler.StagingToken, key graveler.Key) (*graveler.Value, error) {
	var err error
	data := &graveler.StagedEntryData{}
	// workaround for batching DBIO markers objects
	if m.batchDBIOTransactionMarkers && isDBIOTransactionalMarkerObject(key) {
		data, err = m.getBatchedEntryData(ctx, st, key)
	} else {
		_, err = kv.GetMsg(ctx, m.kvStore, graveler.StagingTokenPartition(st), key, data)
	}

	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = graveler.ErrNotFound
		}
		return nil, err
	}
	// Tombstone handling
	if data.Identity == nil {
		return nil, nil
	}
	return graveler.StagedEntryFromProto(data), nil
}

func (m *Manager) Set(ctx context.Context, st graveler.StagingToken, key graveler.Key, value *graveler.Value, requireExists bool) error {
	// Tombstone handling
	if value == nil {
		value = new(graveler.Value)
	} else if value.Identity == nil {
		return graveler.ErrInvalidValue
	}

	pb := graveler.ProtoFromStagedEntry(key, value)
	stPartition := graveler.StagingTokenPartition(st)
	if requireExists {
		return kv.SetMsgIf(ctx, m.kvStore, stPartition, key, pb, kv.PrecondConditionalExists)
	}
	return kv.SetMsg(ctx, m.kvStore, stPartition, key, pb)
}

func (m *Manager) Update(ctx context.Context, st graveler.StagingToken, key graveler.Key, updateFunc graveler.ValueUpdateFunc) error {
	oldValueProto := &graveler.StagedEntryData{}
	var oldValue *graveler.Value
	pred, err := kv.GetMsg(ctx, m.kvStore, graveler.StagingTokenPartition(st), key, oldValueProto)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			oldValue = nil
		} else {
			return err
		}
	} else {
		oldValue = graveler.StagedEntryFromProto(oldValueProto)
	}
	updatedValue, err := updateFunc(oldValue)
	if err != nil {
		// report error or skip if ErrSkipValueUpdate
		if errors.Is(err, graveler.ErrSkipValueUpdate) {
			return nil
		}
		return err
	}
	return kv.SetMsgIf(ctx, m.kvStore, graveler.StagingTokenPartition(st), key, graveler.ProtoFromStagedEntry(key, updatedValue), pred)
}

func (m *Manager) DropKey(ctx context.Context, st graveler.StagingToken, key graveler.Key) error {
	return m.kvStore.Delete(ctx, []byte(graveler.StagingTokenPartition(st)), key)
}

// List returns an iterator of staged values on the staging token st
func (m *Manager) List(ctx context.Context, st graveler.StagingToken, batchSize int) graveler.ValueIterator {
	return NewStagingIterator(ctx, m.kvStore, st, batchSize)
}

func (m *Manager) Drop(ctx context.Context, st graveler.StagingToken) error {
	return m.DropByPrefix(ctx, st, []byte(""))
}

func (m *Manager) DropAsync(ctx context.Context, st graveler.StagingToken) error {
	err := m.kvStore.Set(ctx, []byte(graveler.CleanupTokensPartition()), []byte(st), []byte("stub-value"))
	select {
	case m.wakeup <- cleanTokens:
	default:
		m.log(ctx).Debug("wakeup channel is full, skipping wakeup")
	}
	return err
}

func (m *Manager) DropByPrefix(ctx context.Context, st graveler.StagingToken, prefix graveler.Key) error {
	return m.dropByPrefix(ctx, m.kvStore, st, prefix)
}

func (m *Manager) dropByPrefix(ctx context.Context, store kv.Store, st graveler.StagingToken, prefix graveler.Key) error {
	itr, err := kv.ScanPrefix(ctx, store, []byte(graveler.StagingTokenPartition(st)), prefix, []byte(""))
	if err != nil {
		return err
	}
	defer itr.Close()
	for itr.Next() {
		err = store.Delete(ctx, []byte(graveler.StagingTokenPartition(st)), itr.Entry().Key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) asyncLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-m.wakeup:
			switch event {
			case cleanTokens:
				err := m.findAndDrop(ctx)
				if err != nil {
					m.log(ctx).WithError(err).Error("Dropping tokens failed")
				} else if m.cleanupCallback != nil {
					m.cleanupCallback()
				}
			default:
				panic(fmt.Sprintf("unknown event: %s", event))
			}
		}
	}
}

// findAndDrop lookup for staging tokens to delete and drop keys by prefix. Uses store limited to rate limit the access.
// it assumes we are processing the data in the background.
func (m *Manager) findAndDrop(ctx context.Context) error {
	const maxTokensToFetch = 512
	it, err := m.kvStoreLimited.Scan(ctx, []byte(graveler.CleanupTokensPartition()), kv.ScanOptions{BatchSize: maxTokensToFetch})
	if err != nil {
		return err
	}
	defer it.Close()

	// Collecting all the tokens so we can shuffle them.
	// Shuffling reduces the chances of 2 servers working on the same staging token
	var stagingTokens [][]byte
	for len(stagingTokens) < maxTokensToFetch && it.Next() {
		stagingTokens = append(stagingTokens, it.Entry().Key)
	}
	rand.Shuffle(len(stagingTokens), func(i, j int) {
		stagingTokens[i], stagingTokens[j] = stagingTokens[j], stagingTokens[i]
	})

	for _, token := range stagingTokens {
		if err := m.dropByPrefix(ctx, m.kvStoreLimited, graveler.StagingToken(token), []byte("")); err != nil {
			return err
		}
		if err := m.kvStoreLimited.Delete(ctx, []byte(graveler.CleanupTokensPartition()), token); err != nil {
			return err
		}
	}

	return it.Err()
}
