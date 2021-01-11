package committed

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/graveler"
)

type Params struct {
	// ApproximateRangeSizeBytes is a target for sizes of range partitions.  It should be
	// kept fixed to maximize re-use of range partitions.
	ApproximateRangeSizeBytes uint64
}

type metaRangeManager struct {
	params       Params
	metaManager  RangeManager // For metaranges
	rangeManager RangeManager // For ranges
}

func NewMetaRangeManager(params Params, metaManager, rangeManager RangeManager) MetaRangeManager {
	return &metaRangeManager{
		params:       params,
		metaManager:  metaManager,
		rangeManager: rangeManager,
	}
}

func (m *metaRangeManager) Exists(ns graveler.StorageNamespace, id graveler.MetaRangeID) (bool, error) {
	return m.metaManager.Exists(Namespace(ns), ID(id))
}

// GetValue finds the matching graveler.ValueRecord in the MetaRange with the rangeID
func (m *metaRangeManager) GetValue(ns graveler.StorageNamespace, id graveler.MetaRangeID, key graveler.Key) (*graveler.ValueRecord, error) {
	// Fetch range containing key.
	v, err := m.metaManager.GetValueGE(Namespace(ns), ID(id), Key(key))
	if errors.Is(err, ErrNotFound) {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("find metarange in %s: %w", id, err)
	}

	rng, err := UnmarshalRange(v.Value)
	if err != nil {
		return nil, fmt.Errorf("unmarshal range data in metarange: %w", err)
	}

	if !(bytes.Compare(rng.MinKey, key) <= 0 && bytes.Compare(key, rng.MaxKey) <= 0) {
		return nil, ErrNotFound
	}

	r, err := m.rangeManager.GetValue(Namespace(ns), rng.ID, Key(key))
	if err != nil {
		return nil, fmt.Errorf("get value in range %s of %s for %s: %w", rng.ID, id, key, err)
	}
	value, err := UnmarshalValue(r.Value)
	if err != nil {
		return nil, err
	}
	return &graveler.ValueRecord{
		Key:   key,
		Value: value,
	}, nil
}

func (m *metaRangeManager) NewWriter(ns graveler.StorageNamespace) MetaRangeWriter {
	return NewGeneralMetaRangeWriter(m.rangeManager, m.metaManager, m.params.ApproximateRangeSizeBytes, Namespace(ns))
}

func (m *metaRangeManager) NewMetaRangeIterator(ns graveler.StorageNamespace, id graveler.MetaRangeID) (Iterator, error) {
	if id == "" {
		return NewEmptyIterator(), nil
	}
	rangesIt, err := m.metaManager.NewRangeIterator(Namespace(ns), ID(id))
	if err != nil {
		return nil, fmt.Errorf("manage metarange %s: %w", id, err)
	}
	return NewIterator(m.rangeManager, Namespace(ns), rangesIt), nil
}
