package kv

import (
	"context"
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

func (s *StoreMessage) Scan(ctx context.Context, prefix string) (*PrimaryIterator, error) {
	return NewPrimaryIterator(ctx, s.Store, prefix)
}

type MessageEntry struct {
	Key   string
	Value protoreflect.ProtoMessage
}

func NewMessageEntry(msg protoreflect.MessageType) *MessageEntry {
	return &MessageEntry{
		Key:   "",
		Value: msg.New().Interface(),
	}
}

type MessageIterator interface {
	Next() bool
	Entry(key *string, value *protoreflect.ProtoMessage) error
	Err() error
	Close()
}

// PrimaryIterator MessageIterator implementation for primary key
// The iterator iterates over the given prefix and returns the proto message and key
type PrimaryIterator struct {
	itr EntriesIterator
	err error
}

func NewPrimaryIterator(ctx context.Context, store Store, prefix string) (*PrimaryIterator, error) {
	itr, err := ScanPrefix(ctx, store, []byte(prefix))
	if err != nil {
		return nil, fmt.Errorf("create prefix iterator: %w", err)
	}
	return &PrimaryIterator{itr: itr}, nil
}

func (m *PrimaryIterator) Next() bool {
	if m.err != nil {
		return false
	}
	return m.itr.Next()
}

func (m *PrimaryIterator) Entry(key *string, value *protoreflect.ProtoMessage) error {
	entry := m.itr.Entry()
	if entry == nil {
		return ErrNotFound
	}
	if value != nil {
		err := proto.Unmarshal(entry.Value, *value)
		if err != nil {
			return fmt.Errorf("unmarshal proto data for key %s: %w", string(entry.Key), err)
		}
	}
	if key != nil {
		*key = string(entry.Key)
	}
	return nil
}

func (m *PrimaryIterator) Err() error {
	if m.err != nil {
		return m.err
	}
	return m.itr.Err()
}

func (m *PrimaryIterator) Close() {
	m.itr.Close()
}

// SecondaryIterator MessageIterator implementation for secondary key
// The iterator iterates over the given prefix, extracts the primary key value from secondary key and then returns
// the proto message and primary key
type SecondaryIterator struct {
	ctx   context.Context
	itr   EntriesIterator
	store Store
	err   error
}

func NewSecondaryIterator(ctx context.Context, store Store, prefix string) (*SecondaryIterator, error) {
	itr, err := ScanPrefix(ctx, store, []byte(prefix))
	if err != nil {
		return nil, fmt.Errorf("create prefix iterator: %w", err)
	}
	return &SecondaryIterator{ctx: ctx, itr: itr, store: store}, nil
}

func (s *SecondaryIterator) Next() bool {
	if s.err != nil {
		return false
	}
	return s.itr.Next()
}

func (s *SecondaryIterator) Entry(key *string, value *protoreflect.ProtoMessage) error {
	secondary := s.itr.Entry()
	if secondary == nil {
		return ErrNotFound
	}

	if value != nil {
		data, err := s.store.Get(s.ctx, secondary.Value)
		if err != nil {
			return fmt.Errorf("getting value from key (secondary key %s, key %s): %w", secondary.Key, secondary.Value, err)
		}
		err = proto.Unmarshal(data.Value, *value)
		if err != nil {
			return fmt.Errorf("unmarshal proto data for key %s: %w", string(secondary.Key), err)
		}
	}
	if key != nil {
		*key = string(secondary.Value)
	}
	return nil
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
