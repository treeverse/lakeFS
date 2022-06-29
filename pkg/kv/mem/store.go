package mem

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/treeverse/lakefs/pkg/kv"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
)

type Driver struct{}

type Store struct {
	m    map[string][]byte
	keys []string
	mu   sync.RWMutex
}

type EntriesIterator struct {
	entry     *kv.Entry
	err       error
	start     string
	partition []byte
	store     *Store
}

const DriverName = "mem"

//nolint:gochecknoinits
func init() {
	kv.Register(DriverName, &Driver{})
}

func (d *Driver) Open(_ context.Context, _ kvparams.KV) (kv.Store, error) {
	return &Store{
		m:    make(map[string][]byte),
		keys: []string{},
	}, nil
}

func combinedKey(partitionKey, key []byte) string {
	return fmt.Sprintf("%s_%s", partitionKey, key)
}

func keyFromCombinedKey(combinedKey string) []byte {
	//nolint:gomnd
	return []byte(strings.SplitN(combinedKey, "_", 2)[1])
}

func partitionKeyFromCombinedKey(combinedKey string) []byte {
	//nolint:gomnd
	return []byte(strings.SplitN(combinedKey, "_", 2)[0])
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

	combinedKey := combinedKey(partitionKey, key)
	value, ok := s.m[combinedKey]
	if !ok {
		return nil, fmt.Errorf("key=%v: %w", combinedKey, kv.ErrNotFound)
	}
	return &kv.ValueWithPredicate{
		Value:     value,
		Predicate: kv.Predicate(value),
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
	combinedKey := combinedKey(partitionKey, key)

	if _, found := s.m[combinedKey]; !found {
		s.insertNewKey(combinedKey)
	}
	s.m[combinedKey] = value
	return nil
}

// insertNewKey insert new key into keys - insert into sorted slice
func (s *Store) insertNewKey(combinedKey string) {
	idx := sort.SearchStrings(s.keys, combinedKey)
	if idx == len(s.keys) {
		s.keys = append(s.keys, combinedKey)
	} else {
		s.keys = append(s.keys, "")
		copy(s.keys[idx+1:], s.keys[idx:])
		s.keys[idx] = combinedKey
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
	combinedKey := combinedKey(partitionKey, key)

	curr, currOK := s.m[combinedKey]
	if valuePredicate == nil {
		if currOK {
			return fmt.Errorf("key=%v: %w", key, kv.ErrPredicateFailed)
		}
		s.insertNewKey(combinedKey)
	} else if !bytes.Equal(valuePredicate.([]byte), curr) {
		return fmt.Errorf("%w: key=%v", kv.ErrPredicateFailed, combinedKey)
	}
	s.m[combinedKey] = value
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
	combinedKey := combinedKey(partitionKey, key)

	if _, found := s.m[combinedKey]; !found {
		return nil
	}
	idx := sort.SearchStrings(s.keys, combinedKey)
	if idx < len(s.keys) && s.keys[idx] == combinedKey {
		s.keys = append(s.keys[:idx], s.keys[idx+1:]...)
	}
	delete(s.m, combinedKey)
	return nil
}

func (s *Store) Scan(_ context.Context, partitionKey, start []byte) (kv.EntriesIterator, error) {
	if len(partitionKey) == 0 {
		return nil, kv.ErrMissingPartitionKey
	}
	combinedKey := combinedKey(partitionKey, start)

	return &EntriesIterator{
		store:     s,
		start:     combinedKey,
		partition: partitionKey,
	}, nil
}

func (s *Store) Close() {}

func (e *EntriesIterator) Next() bool {
	if e.err != nil {
		return false
	}
	if e.start == "" {
		e.entry = nil
		return false
	}
	e.store.mu.RLock()
	defer e.store.mu.RUnlock()
	idx := sort.SearchStrings(e.store.keys, e.start)
	if idx == len(e.store.keys) {
		e.start = ""
		e.entry = nil
		return false
	}
	// point to the entry we found
	combinedKey := e.store.keys[idx]
	value := e.store.m[combinedKey]

	partition := partitionKeyFromCombinedKey(combinedKey)
	if !bytes.Equal(partition, e.partition) {
		e.start = ""
		e.entry = nil
		return false
	}

	e.entry = &kv.Entry{
		PartitionKey: partition,
		Key:          keyFromCombinedKey(combinedKey),
		Value:        value,
	}
	// set start to the next item - nil as end indicator
	if idx+1 < len(e.store.keys) {
		e.start = e.store.keys[idx+1]
	} else {
		e.start = ""
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
