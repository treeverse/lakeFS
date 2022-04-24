package kv

import (
	"context"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type StoreMessage struct {
	Store Store
}

func (s *StoreMessage) GetMsg(ctx context.Context, key []byte, msg protoreflect.ProtoMessage) error {
	val, err := s.Store.Get(ctx, key)
	if err != nil {
		return err
	}
	return proto.Unmarshal(val, msg)
}

func (s *StoreMessage) SetMsg(ctx context.Context, key []byte, msg protoreflect.ProtoMessage) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return s.Store.Set(ctx, key, val)
}

func (s *StoreMessage) SetIf(ctx context.Context, key []byte, msg protoreflect.ProtoMessage, pred protoreflect.ProtoMessage) error {
	curr, err := s.Store.Get(ctx, key)
	if err != nil {
		return err
	}
	currMsg := msg.ProtoReflect().New().Interface()
	err = proto.Unmarshal(curr, currMsg)
	if err != nil {
		return err
	}
	if !proto.Equal(pred, currMsg) {
		return ErrNotFound
	}

	val, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return s.Store.SetIf(ctx, key, val, curr)
}

func (s *StoreMessage) Delete(ctx context.Context, key []byte) error {
	return s.Store.Delete(ctx, key)
}

// TODO: niro - implement when required
// func (s *StoreMessage) Scan(ctx context.Context, start string) (Msgs, error) {
//	//	Scan(ctx context.Context, start []byte) (Entries, error)
//
//}

func (s *StoreMessage) Close() {
	s.Store.Close()
}
