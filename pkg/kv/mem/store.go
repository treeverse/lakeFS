package mem

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/NVIDIA/sortedmap"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
)

type Driver struct{}

// PartitionMap holds key-value pairs of a given partition
type PartitionMap sortedmap.LLRBTree

type Store struct {
	maps map[string]PartitionMap

	mu sync.RWMutex
}

type EntriesIterator struct {
	entry     *kv.Entry
	err       error
	start     []byte
	partition string
	store     *Store
}

func (e *EntriesIterator) SeekGE(key []byte) {
	e.start = key
}

const DriverName = "mem"

//nolint:gochecknoinits
func init() {
	kv.Register(DriverName, &Driver{})
}

func (d *Driver) Open(_ context.Context, _ kvparams.Config) (kv.Store, error) {
	return &Store{
		maps: make(map[string]PartitionMap),
	}, nil
}

func (s *Store) Get(_ context.Context, partitionKey, key []byte) (*kv.ValueWithPredicate, error) {
	if len(partitionKey) == 0 {
		return nil, kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		return nil, kv.ErrMissingKey
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok, err := s.internalGet(partitionKey, key)
	if err != nil {
		return nil, fmt.Errorf("get partition=%s, key=%s: %w", partitionKey, key, err)
	}
	if !ok {
		return nil, fmt.Errorf("partition=%s, key=%s: %w", partitionKey, key, kv.ErrNotFound)
	}

	return &kv.ValueWithPredicate{
		Value:     entry.Value,
		Predicate: kv.Predicate(entry.Value),
	}, nil
}

func (s *Store) internalGet(partitionKey, key []byte) (*kv.Entry, bool, error) {
	m, ok := s.maps[string(partitionKey)]
	if !ok {
		return nil, ok, nil
	}
	value, ok, err := m.GetByKey(key)
	if !ok || err != nil {
		return nil, false, err
	}
	entry := &kv.Entry{
		PartitionKey: partitionKey,
		Key:          key,
		Value:        value.([]byte),
	}
	return entry, true, nil
}

func (s *Store) Set(_ context.Context, partitionKey, key, value []byte) error {
	if len(partitionKey) == 0 {
		return kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		return kv.ErrMissingKey
	}
	if value == nil {
		return kv.ErrMissingValue
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.internalSet(partitionKey, key, value)
}

func (s *Store) internalSet(partitionKey, key, value []byte) error {
	m, ok := s.maps[string(partitionKey)]
	if !ok {
		m = NewPartitionMap()
		s.maps[string(partitionKey)] = m
	}
	// Caller could modify key, value - copy them out.  (Don't use bytes.Clone, which can
	// waste capacity!)
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	ok, err := m.Put(keyCopy, valueCopy)
	if err != nil {
		return err
	}
	if ok {
		// New key.
		return nil
	}
	// Existing key: modify it.
	_, err = m.PatchByKey(keyCopy, valueCopy)
	return err
}

func (s *Store) SetIf(_ context.Context, partitionKey, key, value []byte, valuePredicate kv.Predicate) error {
	if len(partitionKey) == 0 {
		return kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		return kv.ErrMissingKey
	}
	if value == nil {
		return kv.ErrMissingValue
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	curr, currOK, err := s.internalGet(partitionKey, key)
	if err != nil {
		return err
	}

	switch valuePredicate {
	case nil:
		if currOK {
			return fmt.Errorf("key=%s: %w", key, kv.ErrPredicateFailed)
		}

	case kv.PrecondConditionalExists:
		if !currOK {
			return fmt.Errorf("key=%s: %w", key, kv.ErrPredicateFailed)
		}

	default: // check for predicate
		if !bytes.Equal(valuePredicate.([]byte), curr.Value) {
			return fmt.Errorf("%w: partition=%s, key=%s", kv.ErrPredicateFailed, partitionKey, key)
		}
	}

	return s.internalSet(partitionKey, key, value)
}

func (s *Store) Delete(_ context.Context, partitionKey, key []byte) error {
	if len(partitionKey) == 0 {
		return kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		return kv.ErrMissingKey
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	m, ok := s.maps[string(partitionKey)]
	if !ok {
		return nil
	}
	_, err := m.DeleteByKey(key)
	return err
}

func (s *Store) Scan(_ context.Context, partitionKey []byte, options kv.ScanOptions) (kv.EntriesIterator, error) {
	if len(partitionKey) == 0 {
		return nil, kv.ErrMissingPartitionKey
	}

	start := options.KeyStart
	if start == nil {
		start = []byte{}
	}
	return &EntriesIterator{
		store:     s,
		start:     start,
		partition: string(partitionKey),
	}, nil
}

func (s *Store) Close() {}

func (e *EntriesIterator) Next() bool {
	if e.err != nil || e.start == nil { // start is nil only if last iteration we reached end of keys
		return false
	}

	e.store.mu.RLock()
	defer e.store.mu.RUnlock()

	m, ok := e.store.maps[e.partition]
	if !ok {
		e.start = nil
		return false
	}

	// Missing godoc in sortedmap: "Returns index of matching key:value pair or, if no
	// match, index is to key:value just after where this key would go"
	index, _, err := m.BisectRight(e.start)
	if err != nil {
		e.err = err
		return false
	}

	key, value, ok, err := m.GetByIndex(index)
	if err != nil {
		// This is a very strange error: we just got index from BisectRight.  So use a
		// verbose error message.
		e.err = fmt.Errorf("%w for index %d returned by BisectRight", err, index)
		return false
	}
	if !ok {
		// end of iteration
		e.start = nil
		return false
	}
	start := bytes.Clone(key.([]byte))
	start = append(start, 0) // first key after start.
	e.start = start
	e.entry = &kv.Entry{
		PartitionKey: []byte(e.partition),
		Key:          key.([]byte),
		Value:        value.([]byte),
	}
	return true
}

func (e *EntriesIterator) Entry() *kv.Entry {
	return e.entry
}

func (e *EntriesIterator) Err() error {
	return e.err
}

func (e *EntriesIterator) Close() {
	e.err = kv.ErrClosedEntries
}

// partitionMapCompare compares keys by lexicographical byte ordering.
func partitionMapCompare(a, b sortedmap.Key) (int, error) {
	return bytes.Compare(a.([]byte), b.([]byte)), nil
}

// partitionMapDump prints printable versions of keys and values.
type partitionMapDump struct{}

func (d partitionMapDump) DumpKey(k sortedmap.Key) (string, error) {
	return string(k.([]byte)), nil
}

// TODO(ariels): Decode values?  Unfortunately DumpValue does not know the key, which is needed
//
//	to know the type to call kv.NewRecord.
func (d partitionMapDump) DumpValue(v sortedmap.Value) (string, error) {
	return fmt.Sprintf("%q", v.([]byte)), nil
}

func NewPartitionMap() PartitionMap {
	return PartitionMap(sortedmap.NewLLRBTree(partitionMapCompare, partitionMapDump{}))
}
