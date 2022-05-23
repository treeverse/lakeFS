package kv

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// StoreMessage protobuf generic implementation for kv.Store interface applicable for all data models
type StoreMessage struct {
	Store Store
}

func (s *StoreMessage) GetMsg(ctx context.Context, path string, msg protoreflect.ProtoMessage) error {
	val, err := s.Store.Get(ctx, []byte(path))
	if err != nil {
		return fmt.Errorf("failed on Get (path: %s): %w", path, err)
	}
	return proto.Unmarshal(val, msg)
}

func (s *StoreMessage) SetMsg(ctx context.Context, path string, msg protoreflect.ProtoMessage) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed on Set (path: %s): %w", path, err)
	}
	return s.Store.Set(ctx, []byte(path), val)
}

func (s *StoreMessage) SetIf(ctx context.Context, path string, msg protoreflect.ProtoMessage, pred protoreflect.ProtoMessage) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed on Marshal (path: %s): %w", path, err)
	}
	if pred == nil {
		return s.Store.SetIf(ctx, []byte(path), val, nil)
	}

	// Calling Get here is suboptimial and ideally we would save the round trip to the store.
	// Since Proto Marshaling can be inconsistent we must read and parse it first to have a consistent predicate compare.
	// A future optimization here would be adding an ID to the Store interface, or a consistent marshaling protocol.
	curr, err := s.Store.Get(ctx, []byte(path))
	if err != nil {
		return fmt.Errorf("failed on Get. Path (%s): %w", path, err)
	}
	currMsg := msg.ProtoReflect().New().Interface()
	err = proto.Unmarshal(curr, currMsg)
	if err != nil {
		return fmt.Errorf("failed on Unmarshal (path: %s) %w", path, err)
	}
	if !proto.Equal(pred, currMsg) {
		return fmt.Errorf("failed on predicate. Path (%s): %w", path, ErrPredicateFailed)
	}
	return s.Store.SetIf(ctx, []byte(path), val, curr)
}

func (s *StoreMessage) Delete(ctx context.Context, path string) error {
	err := s.Store.Delete(ctx, []byte(path))
	if err != nil {
		err = fmt.Errorf("failed on Delete (path: %s): %w", path, err)
	}
	return err
}

// Scan returns a prefix iterator which returns entries in deserialized format. Scan in store message implementation is msg specific
// Therefore, the given prefix should bound the range of the data only to that which can be deserialized by the given proto message type
func (s *StoreMessage) Scan(ctx context.Context, msg protoreflect.Message, prefix string) (*MessageIterator, error) {
	return NewMessageIterator(ctx, s.Store, msg, prefix)
}

func (s *StoreMessage) Close() {
	s.Store.Close()
}

type MessageIterator struct {
	itr EntriesIterator
	msg protoreflect.Message
	err error
}

type MessageEntry struct {
	Key   string
	Value protoreflect.ProtoMessage
}

func NewMessageIterator(ctx context.Context, store Store, msg protoreflect.Message, prefix string) (*MessageIterator, error) {
	itr, err := ScanPrefix(ctx, store, []byte(prefix))
	if err != nil {
		return nil, fmt.Errorf("create prefix iterator: %w", err)
	}
	return &MessageIterator{itr: itr, msg: msg}, nil
}

func (m *MessageIterator) Next() bool {
	if m.err != nil {
		return false
	}
	return m.itr.Next()
}

func (m *MessageIterator) Entry() *MessageEntry {
	entry := m.itr.Entry()
	if entry == nil {
		return nil
	}
	msg := m.msg.New().Interface()
	err := proto.Unmarshal(entry.Value, msg)
	if err != nil {
		m.err = fmt.Errorf("unmarshal proto data for key %s: %w", string(entry.Key), err)
		return nil
	}
	return &MessageEntry{
		Key:   string(entry.Key),
		Value: msg,
	}
}

func (m *MessageIterator) Err() error {
	if m.err != nil {
		return m.err
	}
	return m.itr.Err()
}

func (m *MessageIterator) Close() {
	m.itr.Close()
}
