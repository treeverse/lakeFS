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
	summary              graveler.DiffSummary
}

// baseGE returns range from base iterator, which is greater or equal than the given key
func (m *merger) baseRangeGE(key graveler.Key) (*Range, error) {
	for {
		_, baseRange := m.base.Value()
		if baseRange != nil && bytes.Compare(baseRange.MaxKey, key) >= 0 {
			return baseRange, nil
		}
		if !m.base.NextRange() {
			break
		}
	}
	if err := m.base.Err(); err != nil {
		return nil, err
	}
	return nil, nil
}

// baseGE returns value from base iterator, which is greater or equal than the given key
func (m *merger) baseKeyGE(key graveler.Key) (*graveler.ValueRecord, error) {
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

// applyAll applies all remaining changes from Iterator to writer
func (m *merger) applyAll(iter Iterator) error {
	for {
		select {
		case <-m.ctx.Done():
			return m.ctx.Err()
		default:
		}
		iterValue, iterRange := iter.Value()
		if iterValue == nil {
			baseRange, err := m.baseRangeGE(graveler.Key(iterRange.MinKey))
			if err != nil {
				return fmt.Errorf("base range GE: %w", err)
			}
			if baseRange == nil || baseRange.ID != iterRange.ID {
				if err := m.writeRange(iterRange); err != nil {
					return err
				}
			}
			if !iter.NextRange() {
				break
			}
		} else {
			baseValue, err := m.baseKeyGE(iterValue.Key)
			if err != nil {
				return fmt.Errorf("base value GE: %w", err)
			}
			if baseValue == nil || !bytes.Equal(baseValue.Identity, iterValue.Identity) {
				if err := m.writeRecord(iterValue); err != nil {
					return err
				}
			}
			if !iter.Next() {
				break
			}
		}
	}
	return iter.Err()
}

// applyBothRanges applies merging when both source and dest on range header
func (m *merger) applyBothRanges(sourceRange *Range, destRange *Range) error {
	switch {
	case sourceRange.ID == destRange.ID: // source and dest added the same range
		err := m.writeRange(sourceRange)
		if err != nil {
			return err
		}
		m.haveSource = m.source.NextRange()
		m.haveDest = m.dest.NextRange()

	case bytes.Compare(sourceRange.MaxKey, destRange.MinKey) < 0: // source before dest
		baseRange, err := m.baseRangeGE(graveler.Key(sourceRange.MinKey))
		if err != nil {
			return fmt.Errorf("base range GE: %w", err)
		}
		if baseRange != nil && sourceRange.ID == baseRange.ID { // dest deleted this range
			m.haveSource = m.source.NextRange()
			return nil
		}
		if baseRange == nil || destRange.ID == baseRange.ID { // source added this range
			err = m.writeRange(sourceRange)
			if err != nil {
				return err
			}
			m.haveSource = m.source.NextRange()
			return nil
		}
		return graveler.ErrConflictFound // conflict - both changed this range

	case bytes.Compare(destRange.MaxKey, sourceRange.MinKey) < 0: // dest before source
		baseRange, err := m.baseRangeGE(graveler.Key(destRange.MinKey))
		if err != nil {
			return fmt.Errorf("base range GE: %w", err)
		}
		if baseRange != nil && sourceRange.ID == baseRange.ID { // dest added this range
			err = m.writeRange(destRange)
			if err != nil {
				return err
			}
			m.haveDest = m.dest.NextRange()
			return nil
		}
		if baseRange == nil || destRange.ID == baseRange.ID { // source deleted this range
			m.haveDest = m.dest.NextRange()
			return nil
		}
		return graveler.ErrConflictFound // conflict - both changed this range

	case bytes.Equal(sourceRange.MinKey, destRange.MinKey) && bytes.Equal(sourceRange.MaxKey, destRange.MaxKey): // same bounds
		baseRange, err := m.baseRangeGE(graveler.Key(sourceRange.MinKey))
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

	default: // ranges overlapping
		m.haveSource = m.source.Next()
		m.haveDest = m.dest.Next()
	}
	return nil
}

// applyBothKeys applies merging when both source and dest insides ranges
func (m *merger) applyBothKeys(sourceValue *graveler.ValueRecord, destValue *graveler.ValueRecord) error {
	c := bytes.Compare(sourceValue.Key, destValue.Key)
	switch {
	case c < 0: // source before dest
		baseValue, err := m.baseKeyGE(sourceValue.Key)
		if err != nil {
			return err
		}
		if baseValue != nil && bytes.Equal(sourceValue.Identity, baseValue.Identity) { // dest deleted this record
			m.haveSource = m.source.Next()
		} else {
			if baseValue != nil && bytes.Equal(sourceValue.Key, baseValue.Key) { // conflict
				return graveler.ErrConflictFound
			}
			// source added this record
			err := m.writeRecord(sourceValue)
			if err != nil {
				return fmt.Errorf("write source record: %w", err)
			}
			m.haveSource = m.source.Next()
		}
	case c > 0: // dest before source
		baseValue, err := m.baseKeyGE(destValue.Key)
		if err != nil {
			return err
		}
		if baseValue != nil && bytes.Equal(destValue.Identity, baseValue.Identity) { // source deleted this record
			m.haveDest = m.dest.Next()
		} else {
			if baseValue != nil && bytes.Equal(destValue.Key, baseValue.Key) { // conflict
				return graveler.ErrConflictFound
			}
			// dest added this record
			err := m.writeRecord(destValue)
			if err != nil {
				return fmt.Errorf("write dest record: %w", err)
			}
			m.haveDest = m.dest.Next()
		}
	default: // identical keys
		baseValue, err := m.baseKeyGE(destValue.Key)
		if err != nil {
			return err
		}
		if !bytes.Equal(sourceValue.Identity, destValue.Identity) {
			if baseValue != nil && (bytes.Equal(sourceValue.Identity, baseValue.Identity) ||
				bytes.Equal(destValue.Identity, baseValue.Identity)) {
				err := m.writeRecord(sourceValue)
				if err != nil {
					return fmt.Errorf("write record: %w", err)
				}
				m.haveSource = m.source.Next()
				m.haveDest = m.dest.Next()
				return nil
			} else { // both changed the same key
				return graveler.ErrConflictFound
			}
		}
		// both added the same record
		err = m.writeRecord(sourceValue)
		if err != nil {
			return fmt.Errorf("write record: %w", err)
		}
		m.haveSource = m.source.Next()
		m.haveDest = m.dest.Next()
	}
	return nil
}

// applyDestRangeSourceKey applies merging when source inside range and dest on range header
func (m *merger) applyDestRangeSourceKey(destRange *Range, sourceValue *graveler.ValueRecord) error {
	if bytes.Compare(destRange.MinKey, sourceValue.Key) > 0 { // source before dest range
		baseValue, err := m.baseKeyGE(sourceValue.Key)
		if err != nil {
			return fmt.Errorf("base key GE: %w", err)
		}
		if baseValue != nil && bytes.Equal(sourceValue.Identity, baseValue.Identity) { // dest deleted this record
			m.haveSource = m.source.Next()
		} else {
			if baseValue != nil && bytes.Equal(sourceValue.Key, baseValue.Key) { // conflict
				return graveler.ErrConflictFound
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

	if bytes.Compare(destRange.MaxKey, sourceValue.Key) < 0 { // dest range before source
		baseRange, err := m.baseRangeGE(graveler.Key(destRange.MinKey))
		if err != nil {
			return fmt.Errorf("base range GE: %w", err)
		}
		if baseRange != nil && destRange.ID == baseRange.ID { // source deleted this range
			m.haveDest = m.dest.NextRange()
			return nil
		} else {
			return graveler.ErrConflictFound // conflict
		}
	}
	// dest is at start of range which we need to scan, enter it
	m.haveDest = m.dest.Next()
	return nil
}

// applySourceRangeDestKey applies merging when dest inside range and source on range header
func (m *merger) applySourceRangeDestKey(sourceRange *Range, destValue *graveler.ValueRecord) error {
	if bytes.Compare(sourceRange.MinKey, destValue.Key) > 0 { // dest before source range
		baseValue, err := m.baseKeyGE(destValue.Key)
		if err != nil {
			return fmt.Errorf("base key GE: %w", err)
		}
		if baseValue != nil && bytes.Equal(destValue.Identity, baseValue.Identity) { // source deleted this record
			m.haveSource = m.source.Next()
		} else {
			if baseValue != nil && bytes.Equal(destValue.Key, baseValue.Key) { // conflict
				return graveler.ErrConflictFound
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

	if bytes.Compare(sourceRange.MaxKey, destValue.Key) < 0 { // source range before dest
		baseRange, err := m.baseRangeGE(graveler.Key(sourceRange.MinKey))
		if err != nil {
			return fmt.Errorf("base range GE: %w", err)
		}
		if baseRange != nil && sourceRange.ID == baseRange.ID { // dest deleted this range
			m.haveSource = m.source.NextRange()
			return nil
		} else {
			return graveler.ErrConflictFound // conflict
		}
	}
	// source is at start of range which we need to scan, enter it
	m.haveSource = m.source.Next()
	return nil
}

func (m *merger) merge() error {
	m.haveSource, m.haveDest, _ = m.source.Next(), m.dest.Next(), m.base.Next()
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
		case sourceValue == nil && destValue == nil:
			err = m.applyBothRanges(sourceRange, destRange)
		case destValue == nil && sourceValue != nil:
			err = m.applyDestRangeSourceKey(destRange, sourceValue)
		case sourceValue == nil && destValue != nil:
			err = m.applySourceRangeDestKey(sourceRange, destValue)
		default:
			err = m.applyBothKeys(sourceValue, destValue)
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
		if err := m.applyAll(m.source); err != nil {
			return err
		}
	}
	if m.haveDest {
		if err := m.applyAll(m.dest); err != nil {
			return err
		}
	}
	return nil
}

func Merge(ctx context.Context, writer MetaRangeWriter, base Iterator, source Iterator, destination Iterator) (graveler.DiffSummary, error) {
	m := merger{
		ctx:     ctx,
		logger:  logging.FromContext(ctx),
		writer:  writer,
		base:    base,
		source:  source,
		dest:    destination,
		summary: graveler.DiffSummary{Count: make(map[graveler.DiffType]int), Incomplete: true}, // Incomplete because skipping ranges
	}
	return m.summary, m.merge()
}
