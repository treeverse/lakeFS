package committed

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/logging"
)

type GeneralMetaRangeWriter struct {
	ctx              context.Context
	params           *Params // for breaking ranges
	namespace        Namespace
	metaRangeManager RangeManager
	rangeManager     RangeManager
	rangeWriter      RangeWriter // writer for the current range
	lastKey          Key
	batchWriteCloser BatchWriterCloser
	ranges           []Range
}

var (
	ErrUnsortedKeys = errors.New("keys should be written in ascending order")
	ErrNilValue     = errors.New("record value should not be nil")
)

func NewGeneralMetaRangeWriter(ctx context.Context, rangeManager, metaRangeManager RangeManager, params *Params, namespace Namespace) *GeneralMetaRangeWriter {
	return &GeneralMetaRangeWriter{
		ctx:              ctx,
		rangeManager:     rangeManager,
		metaRangeManager: metaRangeManager,
		batchWriteCloser: NewBatchCloser(),
		params:           params,
		namespace:        namespace,
	}
}

// WriteRecord writes a record to the current range, decides if should close range
func (w *GeneralMetaRangeWriter) WriteRecord(record graveler.ValueRecord) error {
	if w.lastKey != nil && bytes.Compare(record.Key, w.lastKey) <= 0 {
		return ErrUnsortedKeys
	}
	if record.Value == nil {
		return ErrNilValue
	}

	var err error
	if w.rangeWriter == nil {
		w.rangeWriter, err = w.rangeManager.GetWriter(w.ctx, w.namespace)
		if err != nil {
			return fmt.Errorf("get range writer: %w", err)
		}
	}

	v, err := MarshalValue(record.Value)
	if err != nil {
		return err
	}
	err = w.rangeWriter.WriteRecord(Record{Key: Key(record.Key), Value: v})
	if err != nil {
		return fmt.Errorf("write record to range: %w", err)
	}
	w.lastKey = Key(record.Key)

	if w.shouldBreakAtKey(record.Key) {
		return w.closeCurrentRange()
	}
	return nil
}

func (w *GeneralMetaRangeWriter) closeCurrentRange() error {
	if w.rangeWriter == nil {
		return nil
	}
	if err := w.batchWriteCloser.CloseWriterAsync(w.rangeWriter); err != nil {
		return fmt.Errorf("write range: %w", err)
	}
	w.rangeWriter = nil
	return nil
}

func (w *GeneralMetaRangeWriter) getBatchedRanges() ([]Range, error) {
	wr, err := w.batchWriteCloser.Wait()
	if err != nil {
		return nil, fmt.Errorf("batch write closer wait: %w", err)
	}
	ranges := make([]Range, len(wr))
	for i, r := range wr {
		ranges[i] = Range{
			ID:            r.RangeID,
			MinKey:        r.First,
			MaxKey:        r.Last,
			EstimatedSize: r.EstimatedRangeSizeBytes,
		}
	}
	return ranges, nil
}

func (w *GeneralMetaRangeWriter) WriteRange(rng Range) error {
	if w.lastKey != nil && bytes.Compare(rng.MinKey, w.lastKey) <= 0 {
		return ErrUnsortedKeys
	}
	if err := w.closeCurrentRange(); err != nil {
		return err
	}
	w.lastKey = rng.MaxKey
	w.ranges = append(w.ranges, rng)
	return nil
}

func (w *GeneralMetaRangeWriter) Close() (*graveler.MetaRangeID, error) {
	if err := w.closeCurrentRange(); err != nil {
		return nil, err
	}
	ranges, err := w.getBatchedRanges()
	if err != nil {
		return nil, err
	}
	ranges = append(ranges, w.ranges...)
	sort.Slice(ranges, func(i, j int) bool {
		return bytes.Compare(ranges[i].MaxKey, ranges[j].MaxKey) < 0
	})
	w.ranges = ranges
	return w.writeRangesToMetaRange()
}

// shouldBreakAtKey returns true if should break range after the given key
func (w *GeneralMetaRangeWriter) shouldBreakAtKey(key graveler.Key) bool {
	approximateSize := w.rangeWriter.GetApproximateSize()
	if approximateSize < w.params.MinRangeSizeBytes {
		return false
	}
	if approximateSize >= w.params.MaxRangeSizeBytes {
		return true
	}

	h := fnv.New64a()
	// FNV always reads all bytes and never fails; ignore its return values
	_, _ = h.Write(key)
	r := h.Sum64() % uint64(w.params.RangeSizeRaggedness)
	return r == 0
}

// writeRangesToMetaRange writes all ranges to a MetaRange and returns the MetaRangeID
func (w *GeneralMetaRangeWriter) writeRangesToMetaRange() (*graveler.MetaRangeID, error) {
	metaRangeWriter, err := w.metaRangeManager.GetWriter(w.ctx, w.namespace)
	if err != nil {
		return nil, fmt.Errorf("failed creating metarange writer: %w", err)
	}
	defer func() {
		if abortErr := metaRangeWriter.Abort(); abortErr != nil {
			logging.Default().WithField("namespace", w.namespace).Errorf("failed aborting metarange writer: %w", err)
		}
	}()
	for _, p := range w.ranges {
		rangeValue, err := rangeToValue(p)
		if err != nil {
			return nil, err
		}
		if err := metaRangeWriter.WriteRecord(Record{Key: p.MaxKey, Value: rangeValue}); err != nil {
			return nil, fmt.Errorf("failed writing range to metarange writer: %w", err)
		}
	}
	wr, err := metaRangeWriter.Close()
	if err != nil {
		return nil, fmt.Errorf("failed closing metarange writer: %w", err)
	}
	metaRangeID := graveler.MetaRangeID(wr.RangeID)
	return &metaRangeID, nil
}

func (w *GeneralMetaRangeWriter) Abort() error {
	if w.rangeWriter == nil {
		return nil
	}
	return w.rangeWriter.Abort()
}
