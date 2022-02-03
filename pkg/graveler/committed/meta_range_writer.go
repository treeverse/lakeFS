package committed

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
)

type BaseMetaRangeWriter struct {
	ctx               context.Context
	metadata          graveler.Metadata
	params            *Params // for breaking ranges
	breakByRaggedness func(key graveler.Key) bool
	namespace         Namespace
	metaRangeManager  RangeManager
	rangeManager      RangeManager
	rangeWriter       RangeWriter // writer for the current range
	lastKey           Key
	batchWriteCloser  BatchWriterCloser
	ranges            []Range
}

const (
	MetadataTypeKey        = "type"
	MetadataRangesType     = "ranges"
	MetadataMetarangesType = "metaranges"
)

var (
	ErrUnsortedKeys = errors.New("keys should be written in ascending order")
	ErrNilValue     = errors.New("record value should not be nil")
)

func NewBaseMetaRangeWriter(ctx context.Context, rangeManager, metaRangeManager RangeManager, params *Params, breakByRaggedness func(key graveler.Key) bool, namespace Namespace, md graveler.Metadata) *BaseMetaRangeWriter {
	return &BaseMetaRangeWriter{
		ctx:               ctx,
		metadata:          md,
		rangeManager:      rangeManager,
		metaRangeManager:  metaRangeManager,
		batchWriteCloser:  NewBatchCloser(params.MaxUploaders),
		params:            params,
		breakByRaggedness: breakByRaggedness,
		namespace:         namespace,
	}
}

// WriteRecord writes a record to the current range, decides if should close range
func (w *BaseMetaRangeWriter) WriteRecord(record graveler.ValueRecord) error {
	if w.lastKey != nil && bytes.Compare(record.Key, w.lastKey) <= 0 {
		return ErrUnsortedKeys
	}
	if record.Value == nil {
		return ErrNilValue
	}

	var err error
	if w.rangeWriter == nil {
		w.rangeWriter, err = w.rangeManager.GetWriter(w.ctx, w.namespace, w.metadata)
		if err != nil {
			return fmt.Errorf("get range writer: %w", err)
		}
		w.rangeWriter.SetMetadata(MetadataTypeKey, MetadataRangesType)
	}

	v, err := MarshalValue(record.Value)
	if err != nil {
		return err
	}
	err = w.rangeWriter.WriteRecord(Record{Key: Key(record.Key), Value: v})
	if err != nil {
		return fmt.Errorf("write record to range: %w", err)
	}
	w.lastKey = Key(record.Key.Copy())
	if w.shouldBreakAtKey(record.Key) {
		return w.closeCurrentRange()
	}
	return nil
}

func (w *BaseMetaRangeWriter) closeCurrentRange() error {
	if w.rangeWriter == nil {
		return nil
	}
	if err := w.batchWriteCloser.CloseWriterAsync(w.rangeWriter); err != nil {
		return fmt.Errorf("write range: %w", err)
	}
	w.rangeWriter = nil
	return nil
}

func (w *BaseMetaRangeWriter) getBatchedRanges() ([]Range, error) {
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
			Count:         int64(r.Count),
		}
	}
	return ranges, nil
}

func (w *BaseMetaRangeWriter) WriteRange(rng Range) error {
	if w.lastKey != nil && bytes.Compare(rng.MinKey, w.lastKey) <= 0 {
		return ErrUnsortedKeys
	}
	if err := w.closeCurrentRange(); err != nil {
		return err
	}
	w.lastKey = make(Key, len(rng.MaxKey))
	copy(w.lastKey, rng.MaxKey)
	w.ranges = append(w.ranges, rng)
	return nil
}

func (w *BaseMetaRangeWriter) close() ([]Range, error) {
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
	return w.ranges, nil
}

// shouldBreakAtKey returns true if should break range after the given key
func (w *BaseMetaRangeWriter) shouldBreakAtKey(key graveler.Key) bool {
	approximateSize := w.rangeWriter.GetApproximateSize()
	// TODO(Guys): consider removing, might cause problems on parallel
	if approximateSize < w.params.MinRangeSizeBytes {
		return false
	}
	if approximateSize >= w.params.MaxRangeSizeBytes {
		return true
	}
	return w.breakByRaggedness(key)
}

// writeRangesToMetaRange writes all ranges to a MetaRange and returns the MetaRangeID
func writeRangesToMetaRange(ctx context.Context, namespace Namespace, metadata graveler.Metadata, manager RangeManager, ranges []Range) (*graveler.MetaRangeID, error) {
	metaRangeWriter, err := manager.GetWriter(ctx, namespace, metadata)
	if err != nil {
		return nil, fmt.Errorf("failed creating metarange writer: %w", err)
	}

	// write user provided metadata, if any
	for k, v := range metadata {
		metaRangeWriter.SetMetadata(k, v)
	}
	// set type
	metaRangeWriter.SetMetadata(MetadataTypeKey, MetadataMetarangesType)

	defer func() {
		if abortErr := metaRangeWriter.Abort(); abortErr != nil {
			logging.Default().WithField("namespace", namespace).Errorf("failed aborting metarange writer: %w", err)
		}
	}()
	for _, p := range ranges {
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

func (w *BaseMetaRangeWriter) Abort() error {
	if w.rangeWriter == nil {
		return nil
	}
	return w.rangeWriter.Abort()
}

type GeneralMetaRangeWriterCloser struct {
	*BaseMetaRangeWriter
}

func (g *GeneralMetaRangeWriterCloser) Close() (*graveler.MetaRangeID, error) {
	ranges, err := g.close()
	if err != nil {
		return nil, err
	}
	return writeRangesToMetaRange(g.ctx, g.namespace, g.metadata, g.metaRangeManager, ranges)
}

func NewGeneralMetaRangeWriter(ctx context.Context, rangeManager, metaRangeManager RangeManager, params *Params, breakByRaggedness func(key graveler.Key) bool, namespace Namespace, md graveler.Metadata) *GeneralMetaRangeWriterCloser {
	baseMetaRangeWriter := NewBaseMetaRangeWriter(ctx,
		rangeManager,
		metaRangeManager,
		params,
		breakByRaggedness,
		namespace,
		md)
	return &GeneralMetaRangeWriterCloser{
		BaseMetaRangeWriter: baseMetaRangeWriter,
	}
}

type GeneralMetaRangeWriterPartCloser struct {
	*BaseMetaRangeWriter
}

func NewGeneralMetaRangeWriterPartCloser(ctx context.Context, rangeManager, metaRangeManager RangeManager, params *Params, breakByRaggedness func(key graveler.Key) bool, namespace Namespace, md graveler.Metadata) *GeneralMetaRangeWriterPartCloser {
	baseMetaRangeWriter := NewBaseMetaRangeWriter(ctx,
		rangeManager,
		metaRangeManager,
		params,
		breakByRaggedness,
		namespace,
		md)
	return &GeneralMetaRangeWriterPartCloser{
		BaseMetaRangeWriter: baseMetaRangeWriter,
	}
}

func (g *GeneralMetaRangeWriterPartCloser) ClosePart() ([]Range, error) {
	return g.close()
}

func CompleteMultipartMetaRange(ctx context.Context, namespace Namespace, metadata graveler.Metadata, manager RangeManager, ranges []Range) (*graveler.MetaRangeID, error) {
	sort.Slice(ranges, func(i, j int) bool {
		return bytes.Compare(ranges[i].MaxKey, ranges[j].MaxKey) < 0
	})
	return writeRangesToMetaRange(ctx, namespace, metadata, manager, ranges)
}
