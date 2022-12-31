package mem

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"sync"

	"github.com/treeverse/lakefs/pkg/kv"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
)

type Driver struct{}

// PartitionMap holds key-value pairs of a given partition
type PartitionMap map[string]kv.Entry

type Store struct {
	m map[string]PartitionMap

	mu sync.RWMutex
}

type EntriesIterator struct {
	entry     *kv.Entry
	err       error
	start     []byte
	partition string
	store     *Store
}

const DriverName = "mem"

//nolint:gochecknoinits
func init() {
	kv.Register(DriverName, &Driver{})
}

func (d *Driver) Open(_ context.Context, _ kvparams.Config) (kv.Store, error) {
	return &Store{
		m: make(map[string]PartitionMap, 0),
	}, nil
}

func encodeKey(key []byte) string {
	return base64.StdEncoding.EncodeToString(key)
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

	sKey := encodeKey(key)
	value, ok := s.m[string(partitionKey)][sKey]
	if !ok {
		return nil, fmt.Errorf("partition=%s, key=%v, encoding=%s: %w", partitionKey, key, sKey, kv.ErrNotFound)
	}
	return &kv.ValueWithPredicate{
		Value:     value.Value,
		Predicate: kv.Predicate(value.Value),
	}, nil
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

	s.internalSet(partitionKey, key, value)

	return nil
}

func (s *Store) internalSet(partitionKey, key, value []byte) {
	sKey := encodeKey(key)
	if _, ok := s.m[string(partitionKey)]; !ok {
		s.m[string(partitionKey)] = make(map[string]kv.Entry)
	}
	s.m[string(partitionKey)][sKey] = kv.Entry{
		PartitionKey: partitionKey,
		Key:          key,
		Value:        value,
	}
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

	sKey := encodeKey(key)
	curr, currOK := s.m[string(partitionKey)][sKey]
	if valuePredicate == nil {
		if currOK {
			return fmt.Errorf("key=%v: %w", key, kv.ErrPredicateFailed)
		}
	} else if !bytes.Equal(valuePredicate.([]byte), curr.Value) {
		return fmt.Errorf("%w: partition=%s, key=%v, encoding=%s", kv.ErrPredicateFailed, partitionKey, key, sKey)
	}

	s.internalSet(partitionKey, key, value)

	return nil
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

	sKey := encodeKey(key)
	if _, ok := s.m[string(partitionKey)][sKey]; !ok {
		return nil
	}
	delete(s.m[string(partitionKey)], sKey)
	return nil
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

	l := make([]*kv.Entry, 0)
	if _, ok := e.store.m[e.partition]; ok {
		for _, v := range e.store.m[e.partition] {
			if bytes.Compare(v.Key, e.start) >= 0 {
				entry := v
				l = append(l, &entry)
			}
		}
	}

	if len(l) == 0 { // No results
		e.start = nil
		return false
	}
	if len(l) == 1 { // only 1 key >= start, set start to nil, so to indicate next call to return false immediately.
		e.start = nil
		e.entry = l[0]
		return true
	}
	sort.Slice(l, func(i, j int) bool { return bytes.Compare(l[i].Key, l[j].Key) < 0 })
	e.start = l[1].Key
	e.entry = l[0]
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
