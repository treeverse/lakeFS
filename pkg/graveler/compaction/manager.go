package compaction

import (
	"bytes"
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
)

type Manager struct {
	metaRangeManager committed.MetaRangeManager
}

func NewCompactionManager(m committed.MetaRangeManager) graveler.CompactionManager {
	return &Manager{
		metaRangeManager: m,
	}
}

func (m *Manager) Get(ctx context.Context, ns graveler.StorageNamespace, mr graveler.MetaRangeID, key graveler.Key) (*graveler.Value, error) {
	it, err := m.metaRangeManager.NewMetaRangeIterator(ctx, ns, mr)
	if err != nil {
		return nil, err
	}
	valIt := committed.NewValueIterator(it)
	defer valIt.Close()
	valIt.SeekGE(key)
	// return the next value
	if !valIt.Next() {
		// error or not found
		if err := valIt.Err(); err != nil {
			return nil, err
		}
		return nil, graveler.ErrNotFound
	}
	// compare the key we found
	rec := valIt.Value()
	if !bytes.Equal(rec.Key, key) {
		return nil, graveler.ErrNotFound
	}
	return rec.Value, nil
}

func (m *Manager) List(ctx context.Context, ns graveler.StorageNamespace, rangeID graveler.MetaRangeID) (graveler.ValueIterator, error) {
	it, err := m.metaRangeManager.NewMetaRangeIterator(ctx, ns, rangeID)
	if err != nil {
		return nil, err
	}
	return committed.NewValueIterator(it), nil
}
