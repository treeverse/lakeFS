package tree

import (
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed"
)

type writer struct {
	m                 committed.PartManager
	currentPartWriter committed.Writer
	lastKey           committed.Key
	namespace         committed.Namespace
	treeWriter        committed.Writer // writer for the tree representation
	breakRatio        int              // indicates when to break the parts
}

var ErrUnsorted = errors.New("record should be bigger then all records previously written to writer")
var ErrNilValue = errors.New("record value should not be nil")

func NewWriter(manager committed.PartManager, breakRatio int, namespace committed.Namespace) Writer {
	return &writer{
		m:                 manager,
		currentPartWriter: nil,
		lastKey:           nil,
		breakRatio:        breakRatio,
		namespace:         namespace,
	}
}

// WriteRecord writes a record to the current part, decides if should close part
func (w *writer) WriteRecord(record graveler.ValueRecord) error {
	if w.lastKey != nil && bytes.Compare(record.Key, w.lastKey) < 0 {
		return ErrUnsorted
	}
	if record.Value == nil {
		return ErrNilValue
	}

	var err error
	if w.currentPartWriter == nil {
		w.currentPartWriter, err = w.m.GetWriter(w.namespace)
		if err != nil {
			return err
		}
	}

	v, err := MarshalValue(record.Value)
	if err != nil {
		return err
	}
	err = w.currentPartWriter.WriteRecord(committed.Record{Key: committed.Key(record.Key), Value: v})
	if err != nil {
		return err
	}
	w.lastKey = committed.Key(record.Key)
	breakPoint, err := w.breakPoint(record.Key)
	if err != nil {
		return err
	}
	if breakPoint {
		if err = w.closeCurrentPart(true); err != nil {
			return err
		}
	}
	return nil
}

func (w *writer) closeCurrentPart(reachedBrakePoint bool) error {
	if w.currentPartWriter == nil {
		return nil
	}
	wr, err := w.currentPartWriter.Close()
	if err != nil {
		return err
	}
	part := Part{
		ID:                wr.PartID,
		MinKey:            wr.First,
		MaxKey:            wr.Last,
		EstimatedSize:     wr.Count,
		ReachedBrakePoint: reachedBrakePoint,
	}
	err = w.writePartToTree(part)
	if err != nil {
		return err
	}
	w.currentPartWriter = nil
	return nil
}

func (w *writer) AddPart(part Part) error {
	if w.lastKey != nil && bytes.Compare(part.MinKey, w.lastKey) < 0 {
		return ErrUnsorted
	}
	if err := w.closeCurrentPart(false); err != nil {
		return err
	}
	if err := w.writePartToTree(part); err != nil {
		return err
	}
	w.lastKey = part.MaxKey
	return nil
}

func (w *writer) SaveTree() (*graveler.TreeID, error) {
	if err := w.closeCurrentPart(false); err != nil {
		return nil, err
	}
	tree, err := w.treeWriter.Close()
	if err != nil {
		return nil, err
	}
	treeID := graveler.TreeID(tree.PartID)
	return &treeID, nil
}

func hashToInt(s []byte) (uint32, error) {
	h := fnv.New32a()
	_, err := h.Write(s)
	if err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}

// breakPoint returns true if should brake part after the given key
func (w *writer) breakPoint(key graveler.Key) (bool, error) {
	h, err := hashToInt(key)
	if ErrUnsorted != nil {
		return false, err
	}
	return int(h)%w.breakRatio == 0, nil
}

// PartToTreeRecord returns a tree record created from
func PartToTreeRecord(part Part) (*committed.Record, error) {
	data, err := MarshalPart(part)
	if err != nil {
		return nil, err
	}
	partValue := &graveler.Value{
		Identity: []byte(part.ID),
		Data:     data,
	}
	data, err = MarshalValue(partValue)
	if err != nil {
		return nil, err
	}
	return &committed.Record{
		Key:   part.MinKey,
		Value: data,
	}, nil
}

// writePartToTree writes the part to the treeWriter
func (w *writer) writePartToTree(part Part) error {
	if w.treeWriter == nil {
		var err error
		if w.treeWriter, err = w.m.GetWriter(w.namespace); err != nil {
			return fmt.Errorf("failed creating treeWriter: %w", err)
		}
	}
	treeRecord, err := PartToTreeRecord(part)
	if err != nil {
		return err
	}
	err = w.treeWriter.WriteRecord(*treeRecord)
	return err
}

func (w *writer) Abort() error {
	// TODO(Guys): change once part Writer has abort
	return nil
}
