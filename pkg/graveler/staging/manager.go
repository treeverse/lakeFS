package staging

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

type Manager struct {
	store kv.StoreMessage
	log   logging.Logger
}

func NewManager(store kv.StoreMessage) *Manager {
	return &Manager{
		store: store,
		log:   logging.Default().WithField("service_name", "staging_manager"),
	}
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
	// Simulate DB behavior - fail if key doesn't exist. See: https://github.com/treeverse/lakeFS/issues/3640
	data := &graveler.StagedEntryData{}
	_, err := m.store.GetMsg(ctx, graveler.StagingTokenPartition(st), key, data)
	if err != nil {
		return err
	}
	return m.store.DeleteMsg(ctx, graveler.StagingTokenPartition(st), key)
}

// List TODO niro: Remove batchSize parameter post KV
// List returns an iterator of staged values on the staging token st
func (m *Manager) List(ctx context.Context, st graveler.StagingToken, _ int) (graveler.ValueIterator, error) {
	return NewStagingIterator(ctx, m.store, st)
}

func (m *Manager) Drop(ctx context.Context, st graveler.StagingToken) error {
	// Wish we had 'drop partition'... https://github.com/treeverse/lakeFS/issues/3628
	// Simple implementation
	return m.DropByPrefix(ctx, st, []byte(""))
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
