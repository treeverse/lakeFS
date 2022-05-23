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

// Scan returns a prefix iterator which returns entries in deserialized format. Scan in store message implementation is msg specific.
//   Therefore, the prefix given should limit the range of the data to those that can be deserialized by the proto message type.
func (s *StoreMessage) Scan(ctx context.Context, msgType protoreflect.MessageType, partitionKey, prefix string) (*MessageIterator, error) {
	return NewMessageIterator(ctx, s.Store, msgType, partitionKey, prefix)
}

type MessageIterator struct {
	itr     EntriesIterator
	msgType protoreflect.MessageType
	err     error
}

type MessageEntry struct {
	Key   string
	Value protoreflect.ProtoMessage
}

func NewMessageIterator(ctx context.Context, store Store, msgType protoreflect.MessageType, partitionKey, prefix string) (*MessageIterator, error) {
	itr, err := ScanPrefix(ctx, store, []byte(partitionKey), []byte(prefix))
	if err != nil {
		return nil, fmt.Errorf("create prefix iterator: %w", err)
	}
	return &MessageIterator{itr: itr, msgType: msgType}, nil
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
	msg := m.msgType.New().Interface()
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
