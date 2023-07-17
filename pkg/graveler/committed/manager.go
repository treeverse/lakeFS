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

type committedManager struct {
	metaRangeManager MetaRangeManager
	RangeManager     RangeManager
	params           *Params
	logger           logging.Logger
}

func NewCommittedManager(m MetaRangeManager, r RangeManager, p Params) graveler.CommittedManager {
	return &committedManager{
		metaRangeManager: m,
		RangeManager:     r,
		params:           &p,
		logger:           logging.Default(),
	}
}

func (c *committedManager) Exists(ctx context.Context, ns graveler.StorageNamespace, id graveler.MetaRangeID) (bool, error) {
	return c.metaRangeManager.Exists(ctx, ns, id)
}

func (c *committedManager) Get(ctx context.Context, ns graveler.StorageNamespace, rangeID graveler.MetaRangeID, key graveler.Key) (*graveler.Value, error) {
	it, err := c.metaRangeManager.NewMetaRangeIterator(ctx, ns, rangeID)
	if err != nil {
		return nil, err
	}
	valIt := NewValueIterator(it)
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

func (c *committedManager) List(ctx context.Context, ns graveler.StorageNamespace, rangeID graveler.MetaRangeID) (graveler.ValueIterator, error) {
	it, err := c.metaRangeManager.NewMetaRangeIterator(ctx, ns, rangeID)
	if err != nil {
		return nil, err
	}
	return NewValueIterator(it), nil
}

func (c *committedManager) WriteRange(ctx context.Context, ns graveler.StorageNamespace, it graveler.ValueIterator) (*graveler.RangeInfo, error) {
	writer, err := c.RangeManager.GetWriter(ctx, Namespace(ns), nil)
	if err != nil {
		return nil, fmt.Errorf("failed creating range writer: %w", err)
	}
	writer.SetMetadata(MetadataTypeKey, MetadataRangesType)

	defer func() {
		if err := writer.Abort(); err != nil {
			c.logger.WithError(err).Error("Aborting write to range")
		}
	}()

	for it.Next() {
		record := *it.Value()
		v, err := MarshalValue(record.Value)
		if err != nil {
			return nil, err
		}

		if err := writer.WriteRecord(Record{Key: Key(record.Key), Value: v}); err != nil {
			return nil, fmt.Errorf("writing record: %w", err)
		}
		if writer.ShouldBreakAtKey(record.Key, c.params) {
			break
		}
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("getting value from iterator: %w", err)
	}

	info, err := writer.Close()
	if err != nil {
		return nil, fmt.Errorf("closing writer: %w", err)
	}

	return &graveler.RangeInfo{
		ID:                      graveler.RangeID(info.RangeID),
		MinKey:                  graveler.Key(info.First),
		MaxKey:                  graveler.Key(info.Last),
		Count:                   info.Count,
		EstimatedRangeSizeBytes: info.EstimatedRangeSizeBytes,
	}, nil
}

func (c *committedManager) WriteMetaRange(ctx context.Context, ns graveler.StorageNamespace, ranges []*graveler.RangeInfo) (*graveler.MetaRangeInfo, error) {
	writer := c.metaRangeManager.NewWriter(ctx, ns, nil)
	defer func() {
		if err := writer.Abort(); err != nil {
			c.logger.WithError(err).Error("Aborting write to meta range")
		}
	}()

	sort.Slice(ranges, func(i, j int) bool {
		return bytes.Compare(ranges[i].MinKey, ranges[j].MinKey) < 0
	})

	for _, r := range ranges {
		if err := writer.WriteRange(Range{
			ID:            ID(r.ID),
			MinKey:        Key(r.MinKey),
			MaxKey:        Key(r.MaxKey),
			EstimatedSize: r.EstimatedRangeSizeBytes,
			Count:         int64(r.Count),
			Tombstone:     false,
		}); err != nil {
			c.logger.WithError(err).Error("Aborting writing range to meta range")
			return nil, fmt.Errorf("writing range: %w", err)
		}
	}

	id, err := writer.Close()
	if err != nil {
		return nil, fmt.Errorf("closing metarange: %w", err)
	}

	return &graveler.MetaRangeInfo{
		ID: *id,
	}, nil
}

func (c *committedManager) WriteMetaRangeByIterator(ctx context.Context, ns graveler.StorageNamespace, it graveler.ValueIterator, metadata graveler.Metadata) (*graveler.MetaRangeID, error) {
	writer := c.metaRangeManager.NewWriter(ctx, ns, metadata)
	defer func() {
		if err := writer.Abort(); err != nil {
			c.logger.WithError(err).Error("Aborting write to meta range")
		}
	}()

	for it.Next() {
		if err := writer.WriteRecord(*it.Value()); err != nil {
			return nil, fmt.Errorf("writing record: %w", err)
		}
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("getting value from iterator: %w", err)
	}
	id, err := writer.Close()
	if err != nil {
		return nil, fmt.Errorf("closing writer: %w", err)
	}

	return id, nil
}

func (c *committedManager) Diff(ctx context.Context, ns graveler.StorageNamespace, left, right graveler.MetaRangeID) (graveler.DiffIterator, error) {
	leftIt, err := c.metaRangeManager.NewMetaRangeIterator(ctx, ns, left)
	if err != nil {
		return nil, err
	}
	rightIt, err := c.metaRangeManager.NewMetaRangeIterator(ctx, ns, right)
	if err != nil {
		return nil, err
	}
	return NewDiffValueIterator(ctx, leftIt, rightIt), nil
}

func (c *committedManager) Merge(ctx context.Context, ns graveler.StorageNamespace, destination, source, base graveler.MetaRangeID, strategy graveler.MergeStrategy) (graveler.MetaRangeID, error) {
	if source == base {
		// no changes on source
		return "", graveler.ErrNoChanges
	}
	if destination == base {
		// changes introduced only on source
		return source, nil
	}

	baseIt, err := c.metaRangeManager.NewMetaRangeIterator(ctx, ns, base)
	if err != nil {
		return "", fmt.Errorf("get base iterator: %w", err)
	}
	defer baseIt.Close()

	destIt, err := c.metaRangeManager.NewMetaRangeIterator(ctx, ns, destination)
	if err != nil {
		return "", fmt.Errorf("get destination iterator: %w", err)
	}
	defer destIt.Close()

	srcIt, err := c.metaRangeManager.NewMetaRangeIterator(ctx, ns, source)
	if err != nil {
		return "", fmt.Errorf("get source iterator: %w", err)
	}
	defer srcIt.Close()

	mwWriter := c.metaRangeManager.NewWriter(ctx, ns, nil)
	defer func() {
		err := mwWriter.Abort()
		if err != nil {
			c.logger.WithError(err).Error("Abort failed after Merge")
		}
	}()

	err = Merge(ctx, mwWriter, baseIt, srcIt, destIt, strategy)
	if err != nil {
		if !errors.Is(err, graveler.ErrUserVisible) {
			err = fmt.Errorf("merge ns=%s id=%s: %w", ns, destination, err)
		}
		return "", err
	}
	newID, err := mwWriter.Close()
	if newID == nil {
		return "", fmt.Errorf("close writer ns=%s id=%s: %w", ns, destination, err)
	}
	return *newID, err
}

func (c *committedManager) Commit(ctx context.Context, ns graveler.StorageNamespace, baseMetaRangeID graveler.MetaRangeID, changes graveler.ValueIterator) (graveler.MetaRangeID, graveler.DiffSummary, error) {
	mwWriter := c.metaRangeManager.NewWriter(ctx, ns, nil)
	defer func() {
		err := mwWriter.Abort()
		if err != nil {
			c.logger.WithError(err).Error("Abort failed after Commit")
		}
	}()
	metaRangeIterator, err := c.metaRangeManager.NewMetaRangeIterator(ctx, ns, baseMetaRangeID)
	summary := graveler.DiffSummary{
		Count: map[graveler.DiffType]int{},
	}
	if err != nil {
		return "", summary, fmt.Errorf("get metarange ns=%s id=%s: %w", ns, baseMetaRangeID, err)
	}
	defer metaRangeIterator.Close()
	summary, err = Commit(ctx, mwWriter, metaRangeIterator, changes, &CommitOptions{})
	if err != nil {
		if !errors.Is(err, graveler.ErrUserVisible) {
			err = fmt.Errorf("commit ns=%s id=%s: %w", ns, baseMetaRangeID, err)
		}
		return "", summary, err
	}
	newID, err := mwWriter.Close()
	if newID == nil {
		return "", summary, fmt.Errorf("close writer ns=%s metarange id=%s: %w", ns, baseMetaRangeID, err)
	}
	return *newID, summary, err
}

func (c *committedManager) Compare(ctx context.Context, ns graveler.StorageNamespace, destination, source, base graveler.MetaRangeID) (graveler.DiffIterator, error) {
	diffIt, err := c.Diff(ctx, ns, destination, source)
	if err != nil {
		return nil, fmt.Errorf("diff: %w", err)
	}
	baseIt, err := c.metaRangeManager.NewMetaRangeIterator(ctx, ns, base)
	if err != nil {
		diffIt.Close()
		return nil, fmt.Errorf("get base iterator: %w", err)
	}
	return NewCompareValueIterator(ctx, NewDiffIteratorWrapper(diffIt), baseIt), nil
}

func (c *committedManager) GetMetaRange(ctx context.Context, ns graveler.StorageNamespace, id graveler.MetaRangeID) (graveler.MetaRangeAddress, error) {
	uri, err := c.metaRangeManager.GetMetaRangeURI(ctx, ns, id)
	return graveler.MetaRangeAddress(uri), err
}

func (c *committedManager) GetRange(ctx context.Context, ns graveler.StorageNamespace, id graveler.RangeID) (graveler.RangeAddress, error) {
	uri, err := c.metaRangeManager.GetRangeURI(ctx, ns, id)
	return graveler.RangeAddress(uri), err
}

func (c *committedManager) GetRangeIDByKey(ctx context.Context, ns graveler.StorageNamespace, id graveler.MetaRangeID, key graveler.Key) (graveler.RangeID, error) {
	if id == "" {
		return "", graveler.ErrNotFound
	}
	r, err := c.metaRangeManager.GetRangeByKey(ctx, ns, id, key)
	if err != nil {
		return "", fmt.Errorf("get range for key: %w", err)
	}
	return graveler.RangeID(r.ID), nil
}

func (c *committedManager) Import(ctx context.Context, ns graveler.StorageNamespace, destination, source graveler.MetaRangeID, importPaths []graveler.ImportPath) (graveler.MetaRangeID, error) {
	return "", nil
}
