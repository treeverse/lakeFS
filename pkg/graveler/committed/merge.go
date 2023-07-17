package committed

import (
	"bytes"
	"context"
	"fmt"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
)

type merger struct {
	ctx    context.Context
	logger logging.Logger

	writer               MetaRangeWriter
	base                 Iterator
	source               Iterator
	dest                 Iterator
	haveSource, haveDest bool
	strategy             graveler.MergeStrategy
}

// getNextGEKey moves base iterator from its current position to the next greater equal value
func (m *merger) getNextGEKey(key graveler.Key) (*graveler.ValueRecord, error) {
	baseValue, _ := m.base.Value()
	if baseValue != nil && bytes.Compare(key, baseValue.Key) <= 0 {
		return baseValue, nil
	}

	for {
		_, baseRange := m.base.Value()
		if baseRange != nil && bytes.Compare(baseRange.MaxKey, key) >= 0 {
			for {
				baseValue, innerRange := m.base.Value()
				if baseValue != nil && bytes.Compare(key, baseValue.Key) <= 0 {
					return baseValue, nil
				}
				if !m.base.Next() || innerRange.ID != baseRange.ID {
					break
				}
			}
		} else if !m.base.NextRange() {
			break
		}
	}
	if err := m.base.Err(); err != nil {
		return nil, err
	}
	return nil, nil
}

// getNextOverlappingFromBase moves base iterator from its current position to the next range overlapping with rangeToOverlap
func (m *merger) getNextOverlappingFromBase(rangeToOverlap *Range) (*Range, error) {
	for {
		_, baseRange := m.base.Value()
		if baseRange != nil && bytes.Compare(baseRange.MaxKey, rangeToOverlap.MinKey) >= 0 {
			if bytes.Compare(baseRange.MinKey, rangeToOverlap.MaxKey) > 0 {
				return nil, nil
			}
			return baseRange, nil
		}
		if !m.base.NextRange() {
			break
		}
	}
	return nil, m.base.Err()
}

// writeRange writes Range using writer
func (m *merger) writeRange(writeRange *Range) error {
	if m.logger.IsTracing() {
		m.logger.WithFields(logging.Fields{
			"from": string(writeRange.MinKey),
			"to":   string(writeRange.MaxKey),
			"ID":   writeRange.ID,
		}).Trace("copy entire range")
	}
	if err := m.writer.WriteRange(*writeRange); err != nil {
		return fmt.Errorf("copy range %s: %w", writeRange.ID, err)
	}
	return nil
}

// writeRecord writes graveler.ValueRecord using writer
func (m *merger) writeRecord(writeValue *graveler.ValueRecord) error {
	if m.logger.IsTracing() {
		m.logger.WithFields(logging.Fields{
			"key": string(writeValue.Key),
			"ID":  string(writeValue.Identity),
		}).Trace("write record")
	}
	if err := m.writer.WriteRecord(*writeValue); err != nil {
		return fmt.Errorf("write record: %w", err)
	}
	return nil
}

func (m *merger) destBeforeSource(destValue *graveler.ValueRecord) error {
	baseValue, err := m.getNextGEKey(destValue.Key)
	if err != nil {
		return err
	}
	if baseValue != nil && bytes.Equal(destValue.Identity, baseValue.Identity) && bytes.Equal(destValue.Key, baseValue.Key) { // source deleted this record
		m.haveDest = m.dest.Next()
	} else {
		if baseValue != nil && bytes.Equal(destValue.Key, baseValue.Key) { // deleted by source changed by dest
			switch m.strategy {
			case graveler.MergeStrategyDest:
				break
			case graveler.MergeStrategySrc:
				m.haveDest = m.dest.Next()
				return nil
			default: // graveler.MergeStrategyNone
				return graveler.ErrConflictFound
			}
		}
		// dest added this record
		err := m.writeRecord(destValue)
		if err != nil {
			return fmt.Errorf("write dest record: %w", err)
		}
		m.haveDest = m.dest.Next()
	}
	return nil
}

func (m *merger) sourceBeforeDest(sourceValue *graveler.ValueRecord) error {
	baseValue, err := m.getNextGEKey(sourceValue.Key)
	if err != nil {
		return err
	}
	if baseValue != nil && bytes.Equal(sourceValue.Identity, baseValue.Identity) && bytes.Equal(sourceValue.Key, baseValue.Key) { // dest deleted this record
		m.haveSource = m.source.Next()
	} else {
		if baseValue != nil && bytes.Equal(sourceValue.Key, baseValue.Key) { // deleted by dest and changed by source
			switch m.strategy {
			case graveler.MergeStrategyDest:
				m.haveSource = m.source.Next()
				return nil
			case graveler.MergeStrategySrc:
				break
			default: // graveler.MergeStrategyNone
				return graveler.ErrConflictFound
			}
		}
		// source added this record
		err := m.writeRecord(sourceValue)
		if err != nil {
			return fmt.Errorf("write source record: %w", err)
		}
		m.haveSource = m.source.Next()
	}
	return nil
}

// handleAll handles the case where only one Iterator from source or dest remains
// Since the iterator can be for either the source ot the dest range, the function
// receives a graveler.MergeStrategy parameter - strategyToInclude - to indicate
// which strategy favors the given range. In case of a conflict, the configured m.strategy
// is compared to the given strategyToInclude, and if they match - the conflict will
// be resolved by taking the value from the given range. If not and the configured
// m.strategy is other than MergeStrategyNone, the record is ignored. If m.strategy is
// MergeStrategyNone - a conflict will be reported
func (m *merger) handleAll(iter Iterator, strategyToInclude graveler.MergeStrategy) error {
	for {
		select {
		case <-m.ctx.Done():
			return m.ctx.Err()
		default:
		}
		iterValue, iterRange := iter.Value()
		if iterValue == nil {
			baseRange, err := m.getNextOverlappingFromBase(iterRange)
			if err != nil {
				return fmt.Errorf("base range GE: %w", err)
			}
			if baseRange == nil || baseRange.ID == iterRange.ID {
				if baseRange == nil {
					if err := m.writeRange(iterRange); err != nil {
						return err
					}
				}
				if !iter.NextRange() {
					break
				}
			} else if !iter.Next() { // need to enter this range
				break
			}
		} else {
			baseValue, err := m.getNextGEKey(iterValue.Key)
			if err != nil {
				return fmt.Errorf("base value GE: %w", err)
			}
			if baseValue == nil || !bytes.Equal(baseValue.Identity, iterValue.Identity) {
				shouldWriteRecord := true
				if baseValue != nil && bytes.Equal(baseValue.Key, iterValue.Key) { // deleted by one changed by iter
					if m.strategy == graveler.MergeStrategyNone { // conflict is only reported if no strategy is selected
						return graveler.ErrConflictFound
					}
					// In case of conflict, if the strategy favors the given iter we
					// still want to write the record. Otherwise, it will be ignored.
					if m.strategy != strategyToInclude {
						shouldWriteRecord = false
					}
				}
				if shouldWriteRecord {
					if err := m.writeRecord(iterValue); err != nil {
						return err
					}
				}
			}
			if !iter.Next() {
				break
			}
		}
	}
	return iter.Err()
}

// handleBothRanges handles the case where both source and dest iterators are at the header of a range
func (m *merger) handleBothRanges(sourceRange *Range, destRange *Range) error {
	switch {
	case sourceRange.ID == destRange.ID: // range hasn't changed or both added the same range
		err := m.writeRange(sourceRange)
		if err != nil {
			return err
		}
		m.haveSource = m.source.NextRange()
		m.haveDest = m.dest.NextRange()

	case sameBoundRanges(sourceRange, destRange):
		baseRange, err := m.getNextOverlappingFromBase(sourceRange)
		if err != nil {
			return err
		}
		if baseRange != nil && (sourceRange.ID == baseRange.ID || destRange.ID == baseRange.ID) {
			if sourceRange.ID == baseRange.ID { // dest added changes
				err = m.writeRange(destRange)
			} else {
				err = m.writeRange(sourceRange) // source added changes
			}
			if err != nil {
				return err
			}
			m.haveSource = m.source.NextRange()
			m.haveDest = m.dest.NextRange()
		} else { // enter both ranges
			m.haveSource = m.source.Next()
			m.haveDest = m.dest.Next()
		}

	case range1BeforeRange2(sourceRange, destRange): // source before dest
		baseRange, err := m.getNextOverlappingFromBase(sourceRange)
		if err != nil {
			return fmt.Errorf("base range GE: %w", err)
		}
		if baseRange == nil { // source added this range
			err = m.writeRange(sourceRange)
			if err != nil {
				return err
			}
			m.haveSource = m.source.NextRange()
			return nil
		}
		if sourceRange.ID == baseRange.ID { // dest deleted this range
			m.haveSource = m.source.NextRange()
			return nil
		}
		// both changed this range
		m.haveSource = m.source.Next()
		m.haveDest = m.dest.Next()

	case range1BeforeRange2(destRange, sourceRange): // dest before source
		baseRange, err := m.getNextOverlappingFromBase(destRange)
		if err != nil {
			return fmt.Errorf("base range GE: %w", err)
		}
		if baseRange == nil { // dest added this range
			err = m.writeRange(destRange)
			if err != nil {
				return err
			}
			m.haveDest = m.dest.NextRange()
			return nil
		}
		if destRange.ID == baseRange.ID { // source deleted this range
			m.haveDest = m.dest.NextRange()
			return nil
		}
		// both changed this range
		m.haveSource = m.source.Next()
		m.haveDest = m.dest.Next()

	default: // ranges overlapping
		m.haveSource = m.source.Next()
		m.haveDest = m.dest.Next()
	}
	return nil
}

func (m *merger) handleConflict(sourceValue *graveler.ValueRecord, destValue *graveler.ValueRecord) error {
	switch m.strategy {
	case graveler.MergeStrategyDest:
		err := m.writeRecord(destValue)
		if err != nil {
			return fmt.Errorf("write record: %w", err)
		}
	case graveler.MergeStrategySrc:
		err := m.writeRecord(sourceValue)
		if err != nil {
			return fmt.Errorf("write record: %w", err)
		}
	default: // graveler.MergeStrategyNone
		return graveler.ErrConflictFound
	}
	m.haveSource = m.source.Next()
	m.haveDest = m.dest.Next()
	return nil
}

// handleBothKeys handles the case where both source and dest iterators are inside range
func (m *merger) handleBothKeys(sourceValue *graveler.ValueRecord, destValue *graveler.ValueRecord) error {
	switch {
	case isValue1BeforeValue2(sourceValue, destValue): // source before dest
		return m.sourceBeforeDest(sourceValue)
	case isValue1BeforeValue2(destValue, sourceValue): // dest before source
		return m.destBeforeSource(destValue)

	default: // identical keys
		baseValue, err := m.getNextGEKey(destValue.Key)
		if err != nil {
			return err
		}
		if !bytes.Equal(sourceValue.Identity, destValue.Identity) {
			if baseValue != nil {
				switch {
				case bytes.Equal(sourceValue.Identity, baseValue.Identity):
					err = m.writeRecord(destValue)
				case bytes.Equal(destValue.Identity, baseValue.Identity):
					err = m.writeRecord(sourceValue)
				default: // both changed the same key
					return m.handleConflict(sourceValue, destValue)
				}
				if err != nil {
					return fmt.Errorf("write record: %w", err)
				}
				m.haveSource = m.source.Next()
				m.haveDest = m.dest.Next()
				return nil
			} else { // both added the same key with different identity
				return m.handleConflict(sourceValue, destValue)
			}
		}
		// record hasn't changed or both added the same record
		err = m.writeRecord(sourceValue)
		if err != nil {
			return fmt.Errorf("write record: %w", err)
		}
		m.haveSource = m.source.Next()
		m.haveDest = m.dest.Next()
	}
	return nil
}

// handleDestRangeSourceKey handles the case where source Iterator inside range and dest Iterator at the header of a range
func (m *merger) handleDestRangeSourceKey(destRange *Range, sourceValue *graveler.ValueRecord) error {
	if bytes.Compare(destRange.MinKey, sourceValue.Key) > 0 { // source before dest range
		return m.sourceBeforeDest(sourceValue)
	}

	if bytes.Compare(destRange.MaxKey, sourceValue.Key) < 0 { // dest range before source
		baseRange, err := m.getNextOverlappingFromBase(destRange)
		if err != nil {
			return fmt.Errorf("base range GE: %w", err)
		}
		if baseRange == nil {
			err = m.writeRange(destRange)
			if err != nil {
				return err
			}
			m.haveDest = m.dest.NextRange()
			return nil
		}
		if destRange.ID == baseRange.ID { // source deleted this range
			m.haveDest = m.dest.NextRange()
			return nil
		}
	}
	// dest is at start of range which we need to scan, enter it
	m.haveDest = m.dest.Next()
	return nil
}

// handleSourceRangeDestKey handles the case where dest Iterator inside range and source Iterator at the header of a range
func (m *merger) handleSourceRangeDestKey(sourceRange *Range, destValue *graveler.ValueRecord) error {
	if bytes.Compare(sourceRange.MinKey, destValue.Key) > 0 { // dest before source range
		return m.destBeforeSource(destValue)
	}

	if bytes.Compare(sourceRange.MaxKey, destValue.Key) < 0 { // source range before dest
		baseRange, err := m.getNextOverlappingFromBase(sourceRange)
		if err != nil {
			return fmt.Errorf("base range GE: %w", err)
		}
		if baseRange == nil {
			err = m.writeRange(sourceRange)
			if err != nil {
				return err
			}
			m.haveSource = m.source.NextRange()
			return nil
		}
		if sourceRange.ID == baseRange.ID { // dest deleted this range
			m.haveSource = m.source.NextRange()
			return nil
		}
	}
	// source is at start of range which we need to scan, enter it
	m.haveSource = m.source.Next()
	return nil
}

func (m *merger) merge() error {
	m.haveSource, m.haveDest, _ = m.source.Next(), m.dest.Next(), m.base.Next()
	if err := m.source.Err(); err != nil {
		return err
	}
	if err := m.dest.Err(); err != nil {
		return err
	}
	if err := m.base.Err(); err != nil {
		return err
	}

	for m.haveSource && m.haveDest {
		select {
		case <-m.ctx.Done():
			return m.ctx.Err()
		default:
		}
		sourceValue, sourceRange := m.source.Value()
		destValue, destRange := m.dest.Value()
		var err error
		switch {
		case rangesDidntStartProcessing(sourceValue, destValue):
			err = m.handleBothRanges(sourceRange, destRange)
		case !isRangeInProgress(destValue) && isRangeInProgress(sourceValue):
			err = m.handleDestRangeSourceKey(destRange, sourceValue)
		case !isRangeInProgress(sourceValue) && isRangeInProgress(destValue):
			err = m.handleSourceRangeDestKey(sourceRange, destValue)
		default:
			err = m.handleBothKeys(sourceValue, destValue)
		}
		if err != nil {
			return err
		}
		if err = m.source.Err(); err != nil {
			return err
		}
		if err = m.dest.Err(); err != nil {
			return err
		}
		if err = m.base.Err(); err != nil {
			return err
		}
	}

	if m.haveSource {
		if err := m.handleAll(m.source, graveler.MergeStrategySrc); err != nil {
			return err
		}
	}
	if m.haveDest {
		if err := m.handleAll(m.dest, graveler.MergeStrategyDest); err != nil {
			return err
		}
	}
	return nil
}

func Merge(ctx context.Context, writer MetaRangeWriter, base Iterator, source Iterator, destination Iterator, strategy graveler.MergeStrategy) error {
	m := merger{
		ctx:      ctx,
		logger:   logging.FromContext(ctx),
		writer:   writer,
		base:     base,
		source:   source,
		dest:     destination,
		strategy: strategy,
	}
	return m.merge()
}

// rangesDidntStartProcessing indicates that all given ranges values represent ranges are about to be processed (not yet
// started)
func rangesDidntStartProcessing(values ...*graveler.ValueRecord) bool {
	for _, val := range values {
		if isRangeInProgress(val) {
			return false
		}
	}
	return true
}

// isRangeInProgress checks if the given range value, which represent the examined range entry, is not nil, which
// indicates that the range is in the middle of processing
func isRangeInProgress(rangeValue *graveler.ValueRecord) bool {
	return rangeValue != nil
}

func sameBoundRanges(range1, range2 *Range) bool {
	return bytes.Equal(range1.MinKey, range2.MinKey) && bytes.Equal(range1.MaxKey, range2.MaxKey)
}

func range1BeforeRange2(range1, range2 *Range) bool {
	return bytes.Compare(range1.MaxKey, range2.MinKey) < 0
}

func isValue1BeforeValue2(v1, v2 *graveler.ValueRecord) bool {
	return bytes.Compare(v1.Key, v2.Key) < 0
}
