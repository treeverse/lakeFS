package mem

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/treeverse/lakefs/pkg/kv"
)

type Driver struct{}

type Store struct {
	m    map[string][]byte
	keys []string
	mu   sync.RWMutex
}

type EntriesIterator struct {
	entry *kv.Entry
	err   error
	start []byte
	store *Store
}

const DriverName = "mem"

//nolint:gochecknoinits
func init() {
	kv.Register(DriverName, &Driver{})
}

func (d *Driver) Open(_ context.Context, _ string) (kv.Store, error) {
	return &Store{
		m:    make(map[string][]byte),
		keys: []string{},
	}, nil
}

func (s *Store) Get(_ context.Context, key []byte) (*kv.ValueWithPredicate, error) {
	if len(key) == 0 {
		return nil, kv.ErrMissingKey
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.m[string(key)]
	if !ok {
		return nil, fmt.Errorf("key=%v: %w", key, kv.ErrNotFound)
	}
	return &kv.ValueWithPredicate{
		Value:     value,
		Predicate: kv.Predicate(value),
	}, nil
}

func (s *Store) Set(_ context.Context, key, value []byte) error {
	if len(key) == 0 {
		return kv.ErrMissingKey
	}
	if value == nil {
		return kv.ErrMissingValue
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, found := s.m[string(key)]; !found {
		s.insertNewKey(key)
	}
	s.m[string(key)] = value
	return nil
}

// insertNewKey insert new key into keys - insert into sorted slice
func (s *Store) insertNewKey(key []byte) {
	idx := sort.SearchStrings(s.keys, string(key))
	if idx == len(s.keys) {
		s.keys = append(s.keys, string(key))
	} else {
		s.keys = append(s.keys, "")
		copy(s.keys[idx+1:], s.keys[idx:])
		s.keys[idx] = string(key)
	}
}

func (s *Store) SetIf(_ context.Context, key, value []byte, valuePredicate kv.Predicate) error {
	if len(key) == 0 {
		return kv.ErrMissingKey
	}
	if value == nil {
		return kv.ErrMissingValue
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	curr, currOK := s.m[string(key)]
	if valuePredicate == nil {
		if currOK {
			return fmt.Errorf("key=%v: %w", key, kv.ErrPredicateFailed)
		}
		s.insertNewKey(key)
	} else if !bytes.Equal(valuePredicate.([]byte), curr) {
		return fmt.Errorf("%w: key=%v", kv.ErrPredicateFailed, key)
	}
	s.m[string(key)] = value
	return nil
}

func (s *Store) Delete(_ context.Context, key []byte) error {
	if len(key) == 0 {
		return kv.ErrMissingKey
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, found := s.m[string(key)]; !found {
		return nil
	}
	idx := sort.SearchStrings(s.keys, string(key))
	if idx < len(s.keys) && s.keys[idx] == string(key) {
		s.keys = append(s.keys[:idx], s.keys[idx+1:]...)
	}
	delete(s.m, string(key))
	return nil
}

func (s *Store) Scan(_ context.Context, start []byte) (kv.EntriesIterator, error) {
	return &EntriesIterator{
		store: s,
		start: start,
	}, nil
}

func (s *Store) Close() {}

func (e *EntriesIterator) Next() bool {
	if e.err != nil {
		return false
	}
	if e.start == nil {
		e.entry = nil
		return false
	}
	e.store.mu.RLock()
	defer e.store.mu.RUnlock()
	idx := sort.SearchStrings(e.store.keys, string(e.start))
	if idx == len(e.store.keys) {
		e.start = nil
		e.entry = nil
		return false
	}
	// point to the entry we found
	key := e.store.keys[idx]
	value := e.store.m[key]
	e.entry = &kv.Entry{
		Key:   []byte(key),
		Value: value,
	}
	// set start to the next item - nil as end indicator
	if idx+1 < len(e.store.keys) {
		e.start = []byte(e.store.keys[idx+1])
	} else {
		e.start = nil
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
