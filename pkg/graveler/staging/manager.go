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

func valueFromProto(pb *graveler.StagedEntry) *graveler.Value {
	return &graveler.Value{
		Identity: pb.Identity,
		Data:     pb.Data,
	}
}

func protoFromValue(key []byte, v *graveler.Value) *graveler.StagedEntry {
	return &graveler.StagedEntry{
		Key:      key,
		Identity: v.Identity,
		Data:     v.Data,
	}
}

func NewManager(store kv.StoreMessage) *Manager {
	return &Manager{
		store: store,
		log:   logging.Default().WithField("service_name", "staging_manager"),
	}
}

func (m *Manager) Get(ctx context.Context, st graveler.StagingToken, key graveler.Key) (*graveler.Value, error) {
	data := &graveler.StagedEntry{}
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
	return valueFromProto(data), nil
}

func (m *Manager) Set(ctx context.Context, st graveler.StagingToken, key graveler.Key, value *graveler.Value, overwrite bool) error {
	// Tombstone handling
	if value == nil {
		value = new(graveler.Value)
	} else if value.Identity == nil {
		return graveler.ErrInvalidValue
	}

	var err error
	pb := protoFromValue(key, value)
	if overwrite {
		err = m.store.SetMsg(ctx, string(st), key, pb)
	} else {
		err = m.store.SetMsgIf(ctx, string(st), key, pb, nil)
		if errors.Is(err, kv.ErrPredicateFailed) {
			return graveler.ErrPreconditionFailed
		}
	}
	return err
}

func (m *Manager) DropKey(ctx context.Context, st graveler.StagingToken, key graveler.Key) error {
	// Simulate DB behavior - fail if key doesn't exist. See: https://github.com/treeverse/lakeFS/issues/3640
	data := &graveler.StagedEntry{}
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
