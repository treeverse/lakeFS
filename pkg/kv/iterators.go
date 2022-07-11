package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type MessageEntry struct {
	Key   []byte
	Value protoreflect.ProtoMessage
}

type MessageIterator interface {
	Next() bool
	Entry() *MessageEntry
	Err() error
	Close()
}

// PrimaryIterator MessageIterator implementation for primary key
// The iterator iterates over the given prefix and returns the proto message and key
type PrimaryIterator struct {
	itr     EntriesIterator
	msgType protoreflect.MessageType
	value   *MessageEntry
	err     error
}

// NewPrimaryIterator - skip=true: returns iterator that scan the set of keys that start with a prefix greater than the after key
// skip=false - returns iterator that scan the set of keys that start with a prefix starting from the after key
func NewPrimaryIterator(ctx context.Context, store Store, msgType protoreflect.MessageType, partitionKey string, prefix, after []byte, skip bool) (*PrimaryIterator, error) {
	itr, err := ScanPrefix(ctx, store, []byte(partitionKey), prefix, after)
	if err != nil {
		return nil, fmt.Errorf("create prefix iterator: %w", err)
	}
	if skip {
		return &PrimaryIterator{itr: NewSkipIterator(itr, after), msgType: msgType}, nil
	}
	return &PrimaryIterator{itr: itr, msgType: msgType}, nil
}

func (i *PrimaryIterator) Next() bool {
	if i.Err() != nil {
		return false
	}
	i.value = nil
	if !i.itr.Next() {
		return false
	}
	entry := i.itr.Entry()
	if entry == nil {
		i.err = ErrNotFound
		return false
	}
	value := i.msgType.New().Interface()
	err := proto.Unmarshal(entry.Value, value)
	if err != nil {
		i.err = fmt.Errorf("unmarshal proto data for key %s: %w", entry.Key, err)
		return false
	}
	i.value = &MessageEntry{
		Key:   entry.Key,
		Value: value,
	}
	return true
}

func (i *PrimaryIterator) Entry() *MessageEntry {
	return i.value
}

func (i *PrimaryIterator) Err() error {
	if i.err != nil {
		return i.err
	}
	return i.itr.Err()
}

func (i *PrimaryIterator) Close() {
	i.itr.Close()
}

// SecondaryIterator MessageIterator implementation for secondary key
// The iterator iterates over the given prefix, extracts the primary key value from secondary key and then returns
// the proto message and primary key
type SecondaryIterator struct {
	ctx          context.Context
	itr          PrimaryIterator
	partitionKey string
	store        Store
	msgType      protoreflect.MessageType
	value        *MessageEntry
	err          error
}

func NewSecondaryIterator(ctx context.Context, store Store, msgType protoreflect.MessageType, partitionKey string, prefix, after []byte) (*SecondaryIterator, error) {
	itr, err := NewPrimaryIterator(ctx, store, (&SecondaryIndex{}).ProtoReflect().Type(), partitionKey, prefix, after, true)
	if err != nil {
		return nil, fmt.Errorf("create prefix iterator: %w", err)
	}
	return &SecondaryIterator{ctx: ctx, itr: *itr, partitionKey: partitionKey, store: store, msgType: msgType}, nil
}

func (s *SecondaryIterator) Next() bool {
	if s.Err() != nil {
		return false
	}
	if !s.itr.Next() {
		return false
	}
	secondary := s.itr.Entry()
	if secondary == nil {
		s.err = ErrNotFound
		return false
	}
	next := secondary.Value.(*SecondaryIndex)

	var (
		primary *ValueWithPredicate
		err     error
	)
	for {
		primary, err = s.store.Get(s.ctx, []byte(s.partitionKey), next.PrimaryKey)
		if !errors.Is(err, ErrNotFound) {
			break
		}
		if !s.itr.Next() {
			return false
		}
		secondary = s.itr.Entry()
		if secondary == nil {
			s.err = ErrNotFound
			return false
		}
		next = secondary.Value.(*SecondaryIndex)
	}
	if err != nil {
		s.err = fmt.Errorf("getting value from key (primary key %s): %w", next.PrimaryKey, err)
		return false
	}
	value := s.msgType.New().Interface()
	err = proto.Unmarshal(primary.Value, value)
	if err != nil {
		s.err = fmt.Errorf("unmarshal proto data for key %s: %w", next.PrimaryKey, err)
		return false
	}
	s.value = &MessageEntry{
		Key:   next.PrimaryKey,
		Value: value,
	}
	return true
}

func (s *SecondaryIterator) Entry() *MessageEntry {
	return s.value
}

func (s *SecondaryIterator) Err() error {
	if s.err != nil {
		return s.err
	}
	return s.itr.Err()
}

func (s *SecondaryIterator) Close() {
	s.itr.Close()
}

// SkipFirstIterator will keep the behaviour of the given EntriesIterator,
// except for skipping the first Entry if its Key is equal to 'after'.
type SkipFirstIterator struct {
	it         EntriesIterator
	after      []byte
	nextCalled bool
}

func NewSkipIterator(it EntriesIterator, after []byte) EntriesIterator {
	return &SkipFirstIterator{it: it, after: after}
}

func (si *SkipFirstIterator) Next() bool {
	if !si.nextCalled {
		si.nextCalled = true
		if !si.it.Next() {
			return false
		}
		if !bytes.Equal(si.it.Entry().Key, si.after) {
			return true
		}
	}
	return si.it.Next()
}

func (si *SkipFirstIterator) Entry() *Entry {
	return si.it.Entry()
}

func (si *SkipFirstIterator) Err() error {
	return si.it.Err()
}

func (si *SkipFirstIterator) Close() {
	si.it.Close()
}

// PartitionIterator Used to scan through a whole partition
type PartitionIterator struct {
	ctx          context.Context
	store        Store
	msgType      protoreflect.MessageType
	itr          EntriesIterator
	partitionKey string
	value        *MessageEntry
	err          error
}

func NewPartitionIterator(ctx context.Context, store Store, msgType protoreflect.MessageType, partitionKey string) (*PartitionIterator, error) {
	itr, err := store.Scan(ctx, []byte(partitionKey), []byte(""))
	if err != nil {
		return nil, err
	}
	return &PartitionIterator{
		ctx:          ctx,
		store:        store,
		msgType:      msgType,
		itr:          itr,
		partitionKey: partitionKey,
	}, nil
}

func (p *PartitionIterator) Next() bool {
	if p.Err() != nil {
		return false
	}
	p.value = nil
	if !p.itr.Next() {
		return false
	}
	entry := p.itr.Entry()
	if entry == nil {
		p.err = ErrMissingValue
		return false
	}
	value := p.msgType.New().Interface()
	err := proto.Unmarshal(entry.Value, value)
	if err != nil {
		p.err = fmt.Errorf("unmarshal proto data for key %s: %w", entry.Key, err)
		return false
	}
	p.value = &MessageEntry{
		Key:   entry.Key,
		Value: value,
	}
	return true
}

func (p *PartitionIterator) SeekGE(key []byte) {
	if p.err == nil {
		p.itr.Close() // Close previous before creating new iterator
		p.itr, p.err = p.store.Scan(p.ctx, []byte(p.partitionKey), key)
	}
}

func (p *PartitionIterator) Entry() *MessageEntry {
	return p.value
}

func (p *PartitionIterator) Err() error {
	if p.err != nil {
		return p.err
	}
	return p.itr.Err()
}

func (p *PartitionIterator) Close() {
	p.itr.Close()
}
