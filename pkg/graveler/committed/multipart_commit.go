package committed

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
)

type result struct {
	ranges  []Range
	summary graveler.DiffSummary
}

type multipartCommitter struct {
	ctx    context.Context
	logger logging.Logger

	parts   []graveler.MultipartCommitPartData
	opts    *CommitOptions
	summary graveler.DiffSummary

	resCh            chan result
	errorCh          chan error
	commitPartDataCh chan graveler.MultipartCommitPartData
	numOfWorkers     int
	namespace        Namespace
	baseMetarangeID  graveler.MetaRangeID
	metaRangeManager MetaRangeManager
}

func (m *multipartCommitter) commitPart(part graveler.MultipartCommitPartData) ([]Range, graveler.DiffSummary, error) {
	mwWriter := m.metaRangeManager.NewPartWriterCloser(m.ctx, graveler.StorageNamespace(m.namespace), nil)
	defer func() {
		err := mwWriter.Abort()
		if err != nil {
			logging.Default().WithError(err).Error("Abort failed after Commit")
		}
	}()
	metaRangeIterator, err := m.metaRangeManager.NewMetaRangeIterator(m.ctx, graveler.StorageNamespace(m.namespace), m.baseMetarangeID)
	boundedIterator := NewTempWrapperBoundedIterator(metaRangeIterator, part.From, part.To)
	summary := graveler.DiffSummary{
		Count: map[graveler.DiffType]int{},
	}
	if err != nil {
		return nil, summary, fmt.Errorf("get metarange ns=%s id=%s: %w", m.namespace, m.baseMetarangeID, err)
	}
	defer metaRangeIterator.Close()
	// TODO(Guys): change to multipart
	summary, err = Commit(m.ctx, mwWriter, boundedIterator, part.Changes, &CommitOptions{})
	if err != nil {
		if !errors.Is(err, graveler.ErrUserVisible) {
			err = fmt.Errorf("commit ns=%s id=%s: %w", m.namespace, m.baseMetarangeID, err)
		}
		return nil, summary, err
	}
	ranges, err := mwWriter.ClosePart()
	if err != nil {
		return nil, summary, err
	}
	return ranges, summary, err
}

func (m *multipartCommitter) worker(jobsChan chan graveler.MultipartCommitPartData, resChan chan result, errChan chan error) {
	for j := range jobsChan {
		ranges, summary, err := m.commitPart(j)
		if err != nil {
			errChan <- err
		} else {
			resChan <- result{
				ranges:  ranges,
				summary: summary,
			}
		}
	}
}

func (m *multipartCommitter) commit() (graveler.MetaRangeID, graveler.DiffSummary, error) {
	// init channels
	m.errorCh = make(chan error)
	m.resCh = make(chan result)
	m.commitPartDataCh = make(chan graveler.MultipartCommitPartData)
	defer func() {
		close(m.resCh)
		close(m.errorCh)
		close(m.commitPartDataCh)
	}()
	// init workers
	for i := 0; i <= m.numOfWorkers; i++ {
		go m.worker(m.commitPartDataCh, m.resCh, m.errorCh)
	}

	newParts := make([]graveler.MultipartCommitPartData, 0)
	for _, part := range m.parts {
		if part.Changes != nil {
			newParts = append(newParts, part)
		}
	}
	// goroutine to send all jobs
	go func(ctx context.Context, parts []graveler.MultipartCommitPartData) {
		for _, part := range parts {
			select {
			case <-ctx.Done():
				return
			default:
			}
			fmt.Printf("sending part from: %s to:%s changes:%s\n", part.From, part.To, part.Changes)
			if part.Changes == nil {
				fmt.Printf("Not sure what happened from %s to %s\n", part.From, part.To)
			} else {
				m.commitPartDataCh <- part
			}
		}
	}(m.ctx, newParts)

	ranges := make([]Range, 0)
	var summary graveler.DiffSummary
	summary.Count = make(map[graveler.DiffType]int, 4) // TODO(Guys): change
	fmt.Printf("newParts length is %d\n", len(newParts))
	for i := 0; i < len(newParts); i++ {
		select {
		case r := <-m.resCh:
			fmt.Printf("%d:Adding %d ranges\n", i, len(r.ranges))
			ranges = append(ranges, r.ranges...)
			summary.Incomplete = summary.Incomplete || r.summary.Incomplete
			summary.Count[graveler.DiffTypeRemoved] += r.summary.Count[graveler.DiffTypeRemoved]
			summary.Count[graveler.DiffTypeAdded] += r.summary.Count[graveler.DiffTypeAdded]
			summary.Count[graveler.DiffTypeChanged] += r.summary.Count[graveler.DiffTypeChanged]
		case err := <-m.errorCh:
			close(m.resCh)
			return "", summary, err
		case <-m.ctx.Done():
			close(m.resCh)
			return "", summary, nil
		}
	}
	fmt.Println("Done")
	// TODO(Guys): call function with RangeManager
	newID, err := m.metaRangeManager.CompleteMultipartMetaRange(m.ctx, m.namespace, nil, ranges)
	if newID == nil {
		return "", summary, fmt.Errorf("close writer ns=%s metarange id=%s: %w", m.namespace, m.metaRangeManager, err)
	}
	return *newID, summary, err
}

func MultipartCommit(ctx context.Context, parts []graveler.MultipartCommitPartData, opts *CommitOptions, numOfWorkers int, baseMetaRangeID graveler.MetaRangeID, namespace Namespace, manager MetaRangeManager) (graveler.MetaRangeID, graveler.DiffSummary, error) {
	m := multipartCommitter{
		ctx:              ctx,
		logger:           logging.FromContext(ctx),
		parts:            parts,
		opts:             opts,
		summary:          graveler.DiffSummary{},
		numOfWorkers:     numOfWorkers,
		namespace:        namespace,
		baseMetarangeID:  baseMetaRangeID,
		metaRangeManager: manager,
	}
	return m.commit()
}

// TODO(Guys): good name might be iteratorSlice
type TempWrapperBoundedIterator struct {
	Iterator
	from, to graveler.Key
}

// TODO(Guys): this is a temporary iterator for

func NewTempWrapperBoundedIterator(iter Iterator, from, to graveler.Key) *TempWrapperBoundedIterator {
	t := &TempWrapperBoundedIterator{
		Iterator: iter,
		from:     from,
		to:       to,
	}
	if from != nil {
		t.SeekGE(from)
	}
	return t
}
func (t *TempWrapperBoundedIterator) SeekGE(id graveler.Key) {
	key := id
	if t.from != nil && bytes.Compare(t.from, id) <= 0 {
		key = t.from
	}
	t.Iterator.SeekGE(key)
}

func (t *TempWrapperBoundedIterator) Next() bool {
	if !t.Iterator.Next() {
		return false
	}
	if t.to == nil {
		return true
	}
	record, rng := t.Iterator.Value()
	if record != nil {
		return bytes.Compare(record.Key, t.to) <= 0
	}
	return bytes.Compare(rng.MinKey, t.to) <= 0
}

func (t *TempWrapperBoundedIterator) NextRange() bool {
	if !t.Iterator.NextRange() {
		return false
	}
	if t.to == nil {
		return true
	}
	record, rng := t.Iterator.Value()
	if record != nil {
		return bytes.Compare(record.Key, t.to) <= 0
	}
	return bytes.Compare(rng.MinKey, t.to) <= 0
}
