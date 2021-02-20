package committed

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/graveler"
)

type Params struct {
	// MinRangeSizeBytes is the smallest size for splitting a range partition as a result
	// of adding a record.  Smaller ranges are still possible due to re-using an existing
	MinRangeSizeBytes uint64
	// MaxRangeSizeBytes is the largest size of a range partition.  In practice the range
	// is split only after an additional record.
	MaxRangeSizeBytes uint64
	// RangeSizeEntriesRaggedness allows raggedness in splitting range partitions.  It is
	// the expected number of records after MinRangeSizeBytes at which to split the range
	// -- ranges are split at the first key with hash divisible by this raggedness.
	RangeSizeEntriesRaggedness float64
	// MaxUploaders is the maximal number of uploaders to use in a single metarange writer.
	MaxUploaders int
}

type metaRangeManager struct {
	params       Params
	metaManager  RangeManager // For metaranges
	rangeManager RangeManager // For ranges
}

var ErrNeedBatchClosers = errors.New("need at least 1 batch uploaded")

func NewMetaRangeManager(params Params, metaManager, rangeManager RangeManager) (MetaRangeManager, error) {
	if params.MaxUploaders < 1 {
		return nil, fmt.Errorf("only %d async closers: %w", params.MaxUploaders, ErrNeedBatchClosers)
	}
	return &metaRangeManager{
		params:       params,
		metaManager:  metaManager,
		rangeManager: rangeManager,
	}, nil
}

func (m *metaRangeManager) Exists(ctx context.Context, ns graveler.StorageNamespace, id graveler.MetaRangeID) (bool, error) {
	return m.metaManager.Exists(ctx, Namespace(ns), ID(id))
}

// GetValue finds the matching graveler.ValueRecord in the MetaRange with the rangeID
func (m *metaRangeManager) GetValue(ctx context.Context, ns graveler.StorageNamespace, id graveler.MetaRangeID, key graveler.Key) (*graveler.ValueRecord, error) {
	// Fetch range containing key.
	v, err := m.metaManager.GetValueGE(ctx, Namespace(ns), ID(id), Key(key))
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

	r, err := m.rangeManager.GetValue(ctx, Namespace(ns), rng.ID, Key(key))
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

func (m *metaRangeManager) NewWriter(ctx context.Context, ns graveler.StorageNamespace, metadata graveler.Metadata) MetaRangeWriter {
	return NewGeneralMetaRangeWriter(ctx, m.rangeManager, m.metaManager, &m.params, Namespace(ns), metadata)
}

func (m *metaRangeManager) NewMetaRangeIterator(ctx context.Context, ns graveler.StorageNamespace, id graveler.MetaRangeID) (Iterator, error) {
	if id == "" {
		return NewEmptyIterator(), nil
	}
	rangesIt, err := m.metaManager.NewRangeIterator(ctx, Namespace(ns), ID(id))
	if err != nil {
		return nil, fmt.Errorf("manage metarange %s: %w", id, err)
	}
	return NewIterator(ctx, m.rangeManager, Namespace(ns), rangesIt), nil
}

func (m *metaRangeManager) GetMetaRangeURI(ctx context.Context, ns graveler.StorageNamespace, id graveler.MetaRangeID) (string, error) {
	return m.metaManager.GetURI(ctx, Namespace(ns), ID(id))
}

func (m *metaRangeManager) GetRangeURI(ctx context.Context, ns graveler.StorageNamespace, id graveler.RangeID) (string, error) {
	return m.rangeManager.GetURI(ctx, Namespace(ns), ID(id))
}
