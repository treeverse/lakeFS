package tree

import (
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/logging"
)

type GeneralWriter struct {
	namespace           committed.Namespace
	treeManager         committed.PartManager
	partManager         committed.PartManager
	partWriter          committed.Writer // writer for the current part
	lastKey             committed.Key
	approximatePartSize uint64 // indicates when to break the parts
	batchWriteCloser    committed.BatchWriterCloser
	parts               []Part
}

var (
	ErrUnsortedKeys = errors.New("keys should be written in ascending order")
	ErrNilValue     = errors.New("record value should not be nil")
)

func NewWriter(partManager, treeManager committed.PartManager, approximatePartSize uint64, namespace committed.Namespace) *GeneralWriter {
	return &GeneralWriter{
		partManager:         partManager,
		treeManager:         treeManager,
		batchWriteCloser:    partManager.GetBatchManager(),
		approximatePartSize: approximatePartSize,
		namespace:           namespace,
	}
}

// WriteRecord writes a record to the current part, decides if should close part
func (w *GeneralWriter) WriteRecord(record graveler.ValueRecord) error {
	if w.lastKey != nil && bytes.Compare(record.Key, w.lastKey) <= 0 {
		return ErrUnsortedKeys
	}
	if record.Value == nil {
		return ErrNilValue
	}

	var err error
	if w.partWriter == nil {
		w.partWriter, err = w.partManager.GetWriter(w.namespace)
		if err != nil {
			return err
		}
	}

	v, err := MarshalValue(record.Value)
	if err != nil {
		return err
	}
	err = w.partWriter.WriteRecord(committed.Record{Key: committed.Key(record.Key), Value: v})
	if err != nil {
		return fmt.Errorf("write record to part: %w", err)
	}
	w.lastKey = committed.Key(record.Key)
	breakpoint, err := w.shouldBreakAtKey(record.Key)
	if err != nil {
		return err
	}
	if breakpoint {
		return w.closeCurrentPart()
	}
	return nil
}

func (w *GeneralWriter) closeCurrentPart() error {
	if w.partWriter == nil {
		return nil
	}
	if err := w.batchWriteCloser.CloseWriterAsync(w.partWriter); err != nil {
		return err
	}
	w.partWriter = nil
	return nil
}

func (w *GeneralWriter) getBatchedParts() ([]Part, error) {
	wr, err := w.batchWriteCloser.Wait()
	if err != nil {
		return nil, fmt.Errorf("batch write closer wait: %w", err)
	}
	parts := make([]Part, len(wr))
	for i, r := range wr {
		parts[i] = Part{
			ID:            r.PartID,
			MinKey:        r.First,
			MaxKey:        r.Last,
			EstimatedSize: r.EstimatedSize,
		}
	}
	return parts, nil
}

func (w *GeneralWriter) AddPart(part Part) error {
	if w.lastKey != nil && bytes.Compare(part.MinKey, w.lastKey) <= 0 {
		return ErrUnsortedKeys
	}
	if err := w.closeCurrentPart(); err != nil {
		return err
	}
	w.lastKey = part.MaxKey
	return nil
}

func (w *GeneralWriter) Close() (*graveler.TreeID, error) {
	if err := w.closeCurrentPart(); err != nil {
		return nil, err
	}
	parts, err := w.getBatchedParts()
	if err != nil {
		return nil, err
	}
	parts = append(parts, w.parts...)
	sort.Slice(parts, func(i, j int) bool {
		return bytes.Compare(parts[i].MinKey, parts[j].MinKey) < 0
	})
	w.parts = parts
	return w.writePartsToTree()
}

// shouldBreakAtKey returns true if should brake part after the given key
func (w *GeneralWriter) shouldBreakAtKey(key graveler.Key) (bool, error) {
	h := fnv.New64a()
	_, err := h.Write(key)
	if err != nil {
		return false, err
	}
	n := h.Sum64() % w.approximatePartSize
	return n == 0, nil
}

// PartToValue returns a Value representing a Part in Tree
func PartToValue(part Part) (committed.Value, error) {
	data, err := MarshalPart(part)
	if err != nil {
		return nil, err
	}
	partValue := &graveler.Value{
		Identity: []byte(part.ID),
		Data:     data,
	}
	return MarshalValue(partValue)
}

// writePartsToTree writes all parts to a tree and returns the TreeID
func (w *GeneralWriter) writePartsToTree() (*graveler.TreeID, error) {
	treeWriter, err := w.treeManager.GetWriter(w.namespace)
	if err != nil {
		return nil, fmt.Errorf("failed creating treeWriter: %w", err)
	}
	for _, p := range w.parts {
		partValue, err := PartToValue(p)
		if err != nil {
			return nil, err
		}
		if err := treeWriter.WriteRecord(committed.Record{Key: p.MinKey, Value: partValue}); err != nil {
			if abortErr := treeWriter.Abort(); abortErr != nil {
				logging.Default().WithField("namespace", w.namespace).Errorf("failed aborting tree writer: %w", err)
			}
			return nil, fmt.Errorf("failed writing part to tree: %w", err)
		}
	}
	wr, err := treeWriter.Close()
	if err != nil {
		return nil, fmt.Errorf("failed closing treeWriter: %w", err)
	}
	treeID := graveler.TreeID(wr.PartID)
	return &treeID, nil
}

func (w *GeneralWriter) Abort() error {
	return w.partWriter.Abort()
}
