package kv

import (
	"context"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// StoreMessage protobuf generic implementation for kv.Store interface applicable for all data models
type StoreMessage struct {
	Store
}

// GetMsg based on 'path' the value will be loaded into 'msg' and return a predicate.
//
//	In case 'msg' is nil, a predicate will be returned
func (s *StoreMessage) GetMsg(ctx context.Context, partitionKey string, key []byte, msg protoreflect.ProtoMessage) (Predicate, error) {
	return GetMsg(ctx, s.Store, partitionKey, key, msg)
}

func (s *StoreMessage) SetMsg(ctx context.Context, partitionKey string, key []byte, msg protoreflect.ProtoMessage) error {
	return SetMsg(ctx, s.Store, partitionKey, key, msg)
}

func (s *StoreMessage) SetMsgIf(ctx context.Context, partitionKey string, key []byte, msg protoreflect.ProtoMessage, predicate Predicate) error {
	return SetMsgIf(ctx, s.Store, partitionKey, key, msg, predicate)
}

func (s *StoreMessage) DeleteMsg(ctx context.Context, partitionKey string, key []byte) error {
	return s.Delete(ctx, []byte(partitionKey), key)
}

func (s *StoreMessage) Scan(ctx context.Context, msgType protoreflect.MessageType, partitionKey string, prefix, after []byte) (*PrimaryIterator, error) {
	return NewPrimaryIterator(ctx, s.Store, msgType, partitionKey, prefix, IteratorOptionsAfter(after))
}

func GetMsg(ctx context.Context, s Store, partitionKey string, key []byte, msg protoreflect.ProtoMessage) (Predicate, error) {
	res, err := s.Get(ctx, []byte(partitionKey), key)
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

func SetMsg(ctx context.Context, s Store, partitionKey string, key []byte, msg protoreflect.ProtoMessage) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return s.Set(ctx, []byte(partitionKey), key, val)
}

func SetMsgIf(ctx context.Context, s Store, partitionKey string, key []byte, msg protoreflect.ProtoMessage, predicate Predicate) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return s.SetIf(ctx, []byte(partitionKey), key, val, predicate)
}
