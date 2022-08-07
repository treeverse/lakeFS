package staging

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

type Manager struct {
	store  kv.StoreMessage
	log    logging.Logger
	wakeup chan bool

	// notifiers are being called with every successful cleanup cycle
	notifiers []chan bool
}

func NewManager(ctx context.Context, store kv.StoreMessage, notifiers ...chan bool) *Manager {
	const wakupChanCapacity = 100
	m := &Manager{
		store:     store,
		log:       logging.Default().WithField("service_name", "staging_manager"),
		wakeup:    make(chan bool, wakupChanCapacity),
		notifiers: notifiers,
	}
	go m.cleanupLoop(ctx)
	return m
}

func (m *Manager) Get(ctx context.Context, st graveler.StagingToken, key graveler.Key) (*graveler.Value, error) {
	data := &graveler.StagedEntryData{}
	_, err := m.store.GetMsg(ctx, graveler.StagingTokenPartition(st), key, data)
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

func (m *Manager) Set(ctx context.Context, st graveler.StagingToken, key graveler.Key, value *graveler.Value, _ bool) error {
	// Tombstone handling
	if value == nil {
		value = new(graveler.Value)
	} else if value.Identity == nil {
		return graveler.ErrInvalidValue
	}

	pb := graveler.ProtoFromStagedEntry(key, value)
	return m.store.SetMsg(ctx, graveler.StagingTokenPartition(st), key, pb)
}

func (m *Manager) Update(ctx context.Context, st graveler.StagingToken, key graveler.Key, updateFunc graveler.ValueUpdateFunc) error {
	oldValueProto := &graveler.StagedEntryData{}
	var oldValue *graveler.Value
	pred, err := m.store.GetMsg(ctx, graveler.StagingTokenPartition(st), key, oldValueProto)
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
		return err
	}
	return m.store.SetMsgIf(ctx, graveler.StagingTokenPartition(st), key, graveler.ProtoFromStagedEntry(key, updatedValue), pred)
}

func (m *Manager) DropKey(ctx context.Context, st graveler.StagingToken, key graveler.Key) error {
	return m.store.DeleteMsg(ctx, graveler.StagingTokenPartition(st), key)
}

// List TODO niro: Remove batchSize parameter post KV
// List returns an iterator of staged values on the staging token st
func (m *Manager) List(ctx context.Context, st graveler.StagingToken, _ int) (graveler.ValueIterator, error) {
	return NewStagingIterator(ctx, m.store, st)
}

func (m *Manager) Drop(ctx context.Context, st graveler.StagingToken) error {
	return m.DropByPrefix(ctx, st, []byte(""))
}

func (m *Manager) DropAsync(ctx context.Context, st graveler.StagingToken) error {
	err := m.store.Store.Set(ctx, []byte(graveler.CleanupTokensPartition()), []byte(st), []byte("stub-value"))
	m.wakeup <- true
	return err
}

func (m *Manager) DropByPrefix(ctx context.Context, st graveler.StagingToken, prefix graveler.Key) error {
	itr, err := kv.ScanPrefix(ctx, m.store.Store, []byte(graveler.StagingTokenPartition(st)), prefix, []byte(""))
	if err != nil {
		return err
	}
	defer itr.Close()
	for itr.Next() {
		err = m.store.Delete(ctx, []byte(graveler.StagingTokenPartition(st)), itr.Entry().Key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) cleanupLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.wakeup:
			err := m.findAndDrop(ctx)
			if err != nil {
				m.log.WithError(err).Error("Dropping tokens failed")
			} else {
				for _, n := range m.notifiers {
					n <- true
				}
			}
		}
	}
}

func (m *Manager) findAndDrop(ctx context.Context) error {
	it, err := m.store.Store.Scan(ctx, []byte(graveler.CleanupTokensPartition()), []byte{})
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Next() {
		if err := m.DropByPrefix(ctx, graveler.StagingToken(it.Entry().Key), []byte("")); err != nil {
			return err
		}
		if err := m.store.Store.Delete(ctx, []byte(graveler.CleanupTokensPartition()), it.Entry().Key); err != nil {
			return err
		}
	}
	return it.Err()
}
