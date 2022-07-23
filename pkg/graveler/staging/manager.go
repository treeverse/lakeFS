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
	_, err := m.store.GetMsg(ctx, string(st), key, data)
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

func (m *Manager) Set(ctx context.Context, st graveler.StagingToken, key graveler.Key, value *graveler.Value, overwrite bool) error {
	// Tombstone handling
	if value == nil {
		value = new(graveler.Value)
	} else if value.Identity == nil {
		return graveler.ErrInvalidValue
	}

	pb := graveler.ProtoFromStagedEntry(key, value)
	if overwrite {
		return m.store.SetMsg(ctx, string(st), key, pb)
	}

	return m.setWithoutOverwrite(ctx, st, key, pb)
}

func (m *Manager) setWithoutOverwrite(ctx context.Context, st graveler.StagingToken, key graveler.Key, pb *graveler.StagedEntryData) error {
	// no overwrite means we need to check if key exists, or value is a tombstone
	oldValue, err := m.Get(ctx, st, key)
	if err != nil && !errors.Is(err, graveler.ErrNotFound) {
		return err
	}

	switch {
	case oldValue != nil && oldValue.Identity != nil:
		// exists but not a tombstone, so we can't overwrite
		return graveler.ErrPreconditionFailed
	case oldValue == nil:
		// doesn't exist, so we can write
		err = m.store.SetMsgIf(ctx, string(st), key, pb, nil)
	case oldValue.Identity == nil:
		// tombstone handling
		tombstoneProto := graveler.ProtoFromStagedEntry(key, new(graveler.Value))
		err = m.store.SetMsgIf(ctx, string(st), key, pb, tombstoneProto)
	}

	if errors.Is(err, kv.ErrPredicateFailed) {
		return graveler.ErrPreconditionFailed
	}
	return err
}

func (m *Manager) DropKey(ctx context.Context, st graveler.StagingToken, key graveler.Key) error {
	// Simulate DB behavior - fail if key doesn't exist. See: https://github.com/treeverse/lakeFS/issues/3640
	data := &graveler.StagedEntryData{}
	_, err := m.store.GetMsg(ctx, string(st), key, data)
	if err != nil {
		return err
	}
	return m.store.DeleteMsg(ctx, string(st), key)
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
	itr, err := kv.ScanPrefix(ctx, m.store.Store, []byte(st), prefix, []byte(""))
	if err != nil {
		return err
	}
	defer itr.Close()
	for itr.Next() {
		err = m.store.Delete(ctx, []byte(st), itr.Entry().Key)
		if err != nil {
			return err
		}
	}

	return nil
}
