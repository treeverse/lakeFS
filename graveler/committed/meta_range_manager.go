package committed

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/logging"
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
	// TODO(ariels): Replace with loggers constructed from context.
	logger logging.Logger
}

func NewPebbleSSTableMetaRangeManager(params Params, metaManager, rangeManager RangeManager) MetaRangeManager {
	return &metaRangeManager{
		params:       params,
		metaManager:  metaManager,
		rangeManager: rangeManager,
		logger:       logging.Default(),
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
	return &graveler.ValueRecord{
		Key:   key,
		Value: value,
	}, nil
}

func (m *metaRangeManager) NewWriter(ns graveler.StorageNamespace) MetaRangeWriter {
	return NewGeneralMetaRangeWriter(m.rangeManager, m.metaManager, m.params.ApproximateRangeSizeBytes, Namespace(ns))
}

func (m *metaRangeManager) NewMetaRangeIterator(ns graveler.StorageNamespace, id graveler.MetaRangeID) (Iterator, error) {
	rangesIt, err := m.metaManager.NewRangeIterator(Namespace(ns), ID(id))
	if err != nil {
		return nil, fmt.Errorf("manage metarange %s: %w", id, err)
	}
	return NewIterator(m.rangeManager, Namespace(ns), &adaptIt{it: rangesIt}), nil
}

func (m *metaRangeManager) execAndLog(f func() error, msg string) {
	if err := f(); err != nil {
		m.logger.WithError(err).Error(msg)
	}
}

// adaptIt adapts a ValueIterator to be a graveler.ValueIterator
type adaptIt struct {
	it  ValueIterator
	err error
}

func (a *adaptIt) Next() bool {
	return a.it.Next()
}

func (a *adaptIt) SeekGE(id graveler.Key) {
	a.it.SeekGE(Key(id))
}

func (a *adaptIt) Value() *graveler.ValueRecord {
	rec := a.it.Value()
	v, err := UnmarshalValue(*&rec.Value)
	if err != nil {
		a.err = err
		return nil
	}
	return &graveler.ValueRecord{
		Key:   graveler.Key(rec.Key),
		Value: v,
	}
}

func (a *adaptIt) Err() error {
	if err := a.it.Err(); err != nil {
		return err
	}
	return a.err
}

func (a *adaptIt) Close() {
	a.Close()
}
