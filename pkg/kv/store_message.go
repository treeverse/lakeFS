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
	v, err := s.Store.Get(ctx, []byte(path))
	if err != nil {
		return err
	}
	return proto.Unmarshal(v, msg)
}

func (s *StoreMessage) GetMsgPredicate(ctx context.Context, path string, msg protoreflect.ProtoMessage) (Predicate, error) {
	vp, err := s.Store.GetValuePredicate(ctx, []byte(path))
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return vp.Predicate, nil
	}
	err = proto.Unmarshal(vp.Value, msg)
	if err != nil {
		return nil, err
	}
	return vp.Predicate, nil
}

func (s *StoreMessage) SetMsg(ctx context.Context, path string, msg protoreflect.ProtoMessage) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return s.Store.Set(ctx, []byte(path), val)
}

func (s *StoreMessage) SetMsgIf(ctx context.Context, path string, msg protoreflect.ProtoMessage, predicate Predicate) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return s.Store.SetIf(ctx, []byte(path), val, predicate)
}

func (s *StoreMessage) Delete(ctx context.Context, path string) error {
	return s.Store.Delete(ctx, []byte(path))
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
