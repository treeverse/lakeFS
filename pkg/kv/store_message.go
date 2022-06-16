package kv

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// StoreMessage protobuf generic implementation for kv.Store interface applicable for all data models
type StoreMessage struct {
	Store
}

// GetMsg based on 'path' the value will be loaded into 'msg' and return a predicate.
//   In case 'msg' is nil, a predicate will be returned
func (s *StoreMessage) GetMsg(ctx context.Context, partitionKey, key string, msg protoreflect.ProtoMessage) (Predicate, error) {
	res, err := s.Get(ctx, []byte(partitionKey), []byte(key))
	if err != nil {
		return nil, err
	}
	// conditional msg - make it work like Get just using key
	if msg == nil {
		return res.Predicate, nil
	}
	err = proto.Unmarshal(res.Value, msg)
	if err != nil {
		return nil, err
	}
	return res.Predicate, nil
}

func (s *StoreMessage) SetMsg(ctx context.Context, partitionKey, key string, msg protoreflect.ProtoMessage) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return s.Set(ctx, []byte(partitionKey), []byte(key), val)
}

func (s *StoreMessage) SetMsgIf(ctx context.Context, partitionKey, key string, msg protoreflect.ProtoMessage, predicate Predicate) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return s.SetIf(ctx, []byte(partitionKey), []byte(key), val, predicate)
}

func (s *StoreMessage) DeleteMsg(ctx context.Context, partitionKey, key string) error {
	return s.Delete(ctx, []byte(partitionKey), []byte(key))
}

func (s *StoreMessage) Scan(ctx context.Context, msgType protoreflect.MessageType, partitionKey, prefix, after string) (*PrimaryIterator, error) {
	return NewPrimaryIterator(ctx, s.Store, msgType, partitionKey, prefix, after)
}

type MessageEntry struct {
	Key   string
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

func NewPrimaryIterator(ctx context.Context, store Store, msgType protoreflect.MessageType, partitionKey, prefix, after string) (*PrimaryIterator, error) {
	itr, err := ScanPrefix(ctx, store, []byte(partitionKey), []byte(prefix), []byte(after))
	if err != nil {
		return nil, fmt.Errorf("create prefix iterator: %w", err)
	}
	return &PrimaryIterator{itr: NewSkipIterator(itr, []byte(after)), msgType: msgType}, nil
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
		Key:   string(entry.Key),
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

func NewSecondaryIterator(ctx context.Context, store Store, msgType protoreflect.MessageType, partitionKey, prefix, after string) (*SecondaryIterator, error) {
	itr, err := NewPrimaryIterator(ctx, store, (&SecondaryIndex{}).ProtoReflect().Type(), partitionKey, prefix, after)
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
		Key:   string(next.PrimaryKey),
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
