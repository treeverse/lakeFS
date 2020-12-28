package tree

import (
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed"
)

type GeneralWriter struct {
	namespace           committed.Namespace
	treeManager         committed.PartManager
	treeWriter          committed.Writer // writer for the tree representation
	partManager         committed.PartManager
	partWriter          committed.Writer // writer for the current part
	partSize            int64
	lastKey             committed.Key
	approximatePartSize uint64 // indicates when to break the parts
}

var (
	ErrUnsortedKeys = errors.New("keys should be written in ascending order")
	ErrNilValue     = errors.New("record value should not be nil")
)

func NewWriter(partManager, treeManager committed.PartManager, approximatePartSize uint64, namespace committed.Namespace) *GeneralWriter {
	return &GeneralWriter{
		partManager:         partManager,
		treeManager:         treeManager,
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
	w.partSize += int64(len(record.Key) + len(v))
	w.lastKey = committed.Key(record.Key)
	breakpoint, err := w.shouldBreakAtKey(record.Key)
	if err != nil {
		return err
	}
	if breakpoint {
		return w.closeCurrentPart(true)
	}
	return nil
}

func (w *GeneralWriter) closeCurrentPart(reachedBreakpoint bool) error {
	if w.partWriter == nil {
		return nil
	}
	wr, err := w.partWriter.Close()
	if err != nil {
		return fmt.Errorf("close part writer: %w", err)
	}
	part := Part{
		ID:                wr.PartID,
		MinKey:            wr.First,
		MaxKey:            wr.Last,
		EstimatedSize:     w.partSize,
		ReachedBreakpoint: reachedBreakpoint,
	}
	err = w.writePartToTree(part)
	if err != nil {
		return fmt.Errorf("write part to tree: %w", err)
	}
	w.partWriter = nil
	w.partSize = 0
	return nil
}

func (w *GeneralWriter) AddPart(part Part) error {
	if w.lastKey != nil && bytes.Compare(part.MinKey, w.lastKey) <= 0 {
		return ErrUnsortedKeys
	}
	if err := w.closeCurrentPart(false); err != nil {
		return err
	}
	if err := w.writePartToTree(part); err != nil {
		return fmt.Errorf("write part to tree: %w", err)
	}
	w.lastKey = part.MaxKey
	return nil
}

func (w *GeneralWriter) Close() (*graveler.TreeID, error) {
	if err := w.closeCurrentPart(false); err != nil {
		return nil, err
	}
	tree, err := w.treeWriter.Close()
	if err != nil {
		return nil, fmt.Errorf("close tree writer: %w", err)
	}
	treeID := graveler.TreeID(tree.PartID)
	return &treeID, nil
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

// writePartToTree writes the part to the treeWriter
func (w *GeneralWriter) writePartToTree(part Part) error {
	if w.treeWriter == nil {
		var err error
		if w.treeWriter, err = w.treeManager.GetWriter(w.namespace); err != nil {
			return fmt.Errorf("failed creating treeWriter: %w", err)
		}
	}
	partValue, err := PartToValue(part)
	if err != nil {
		return err
	}

	return w.treeWriter.WriteRecord(committed.Record{Key: part.MinKey, Value: partValue})
}

func (w *GeneralWriter) Abort() error {
	var res error
	if err := w.partWriter.Abort(); err != nil {
		res = err
	}
	if err := w.treeWriter.Abort(); err != nil {
		res = err
	}
	return res
}
