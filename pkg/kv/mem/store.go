package mem

import (
	"bytes"
	"context"
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

type Entries struct {
	entry *kv.Entry
	err   error
	start []byte
	first bool
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

func (s *Store) Get(_ context.Context, key []byte) ([]byte, error) {
	if key == nil {
		return nil, kv.ErrMissingKey
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.m[string(key)]
	if !ok {
		return nil, kv.ErrNotFound
	}
	return value, nil
}

func (s *Store) Set(_ context.Context, key, value []byte) error {
	if key == nil {
		return kv.ErrMissingKey
	}
	if value == nil {
		return kv.ErrMissingValue
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, found := s.m[string(key)]; !found {
		s.keys = append(s.keys, string(key))
		sort.Strings(s.keys)
	}
	s.m[string(key)] = value
	return nil
}

func (s *Store) SetIf(_ context.Context, key, value, valuePredicate []byte) error {
	if key == nil {
		return kv.ErrMissingKey
	}
	if value == nil {
		return kv.ErrMissingValue
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	curr := s.m[string(key)]
	if valuePredicate == nil {
		if curr != nil {
			return kv.ErrNotFound
		}
		s.keys = append(s.keys, string(key))
		sort.Strings(s.keys)
	} else if !bytes.Equal(valuePredicate, curr) {
		return kv.ErrNotFound
	}
	s.m[string(key)] = value
	return nil
}

func (s *Store) Delete(_ context.Context, key []byte) error {
	if key == nil {
		return kv.ErrMissingKey
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, found := s.m[string(key)]; !found {
		return kv.ErrNotFound
	}
	idx := sort.SearchStrings(s.keys, string(key))
	if idx < len(s.keys) && s.keys[idx] == string(key) {
		s.keys = append(s.keys[:idx], s.keys[idx+1:]...)
	}
	delete(s.m, string(key))
	return nil
}

func (s *Store) Scan(ctx context.Context, start []byte) (kv.Entries, error) {
	return &Entries{
		store: s,
		first: true,
		start: start,
	}, nil
}

func (s *Store) Close() {}

func (e *Entries) Next() bool {
	if e.start == nil {
		return false
	}
	e.store.mu.RLock()
	defer e.store.mu.RUnlock()
	idx := sort.SearchStrings(e.store.keys, string(e.start))
	if idx == len(e.store.keys) {
		e.start = nil
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

func (e *Entries) Entry() *kv.Entry {
	return e.entry
}

func (e *Entries) Err() error {
	return e.err
}

func (e *Entries) Close() {
	e.err = kv.ErrClosedEntries
}
