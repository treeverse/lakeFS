package staging

import (
	"context"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

type Manager struct {
	kvStore        kv.Store
	kvStoreLimited kv.Store
	log            logging.Logger
	wakeup         chan asyncEvent

	// cleanupCallback is being called with every successful cleanup cycle
	cleanupCallback func()
}

// asyncEvent is a type of event to be handled in the async loop
type asyncEvent string

// cleanTokens is async cleaning of deleted staging tokens
const cleanTokens = asyncEvent("clean_tokens")

func NewManager(ctx context.Context, store, storeLimited kv.Store) *Manager {
	const wakeupChanCapacity = 100
	m := &Manager{
		kvStore:        store,
		kvStoreLimited: storeLimited,
		log:            logging.Default().WithField("service_name", "staging_manager"),
		wakeup:         make(chan asyncEvent, wakeupChanCapacity),
	}
	go m.asyncLoop(ctx)
	return m
}

func (m *Manager) OnCleanup(cleanupCallback func()) {
	m.cleanupCallback = cleanupCallback
}

func (m *Manager) Get(ctx context.Context, st graveler.StagingToken, key graveler.Key) (*graveler.Value, error) {
	data := &graveler.StagedEntryData{}
	_, err := kv.GetMsg(ctx, m.kvStore, graveler.StagingTokenPartition(st), key, data)
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
func (m *Manager) List(ctx context.Context, st graveler.StagingToken, batchSize int) (graveler.ValueIterator, error) {
	return NewStagingIterator(ctx, m.kvStore, st, batchSize)
}

func (m *Manager) Drop(ctx context.Context, st graveler.StagingToken) error {
	return m.DropByPrefix(ctx, st, []byte(""))
}

func (m *Manager) DropAsync(ctx context.Context, st graveler.StagingToken) error {
	err := m.kvStore.Set(ctx, []byte(graveler.CleanupTokensPartition()), []byte(st), []byte("stub-value"))
	m.wakeup <- cleanTokens
	return err
}

func (m *Manager) DropByPrefix(ctx context.Context, st graveler.StagingToken, prefix graveler.Key) error {
	itr, err := kv.ScanPrefix(ctx, m.kvStore, []byte(graveler.StagingTokenPartition(st)), prefix, []byte(""))
	if err != nil {
		return err
	}
	defer itr.Close()
	for itr.Next() {
		err = m.kvStore.Delete(ctx, []byte(graveler.StagingTokenPartition(st)), itr.Entry().Key)
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
					m.log.WithError(err).Error("Dropping tokens failed")
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
	it, err := m.kvStoreLimited.Scan(ctx, []byte(graveler.CleanupTokensPartition()), kv.ScanOptions{})
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Next() {
		if err := m.DropByPrefix(ctx, graveler.StagingToken(it.Entry().Key), []byte("")); err != nil {
			return err
		}
		if err := m.kvStoreLimited.Delete(ctx, []byte(graveler.CleanupTokensPartition()), it.Entry().Key); err != nil {
			return err
		}
	}
	return it.Err()
}
