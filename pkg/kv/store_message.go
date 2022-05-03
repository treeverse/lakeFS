package kv

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const PathDelimiter = "/"

// StoreMessage protobuf generic implementation for kv.Store interface applicable for all data models
type StoreMessage struct {
	Store Store
}

func (s *StoreMessage) GetMsg(ctx context.Context, path string, msg protoreflect.ProtoMessage) error {
	val, err := s.Store.Get(ctx, []byte(path))
	if err != nil {
		return fmt.Errorf("failed on Get. Path (%s): %w", path, err)
	}
	return proto.Unmarshal(val, msg)
}

func (s *StoreMessage) SetMsg(ctx context.Context, path string, msg protoreflect.ProtoMessage) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed on Set. Path (%s): %w", path, err)
	}
	return s.Store.Set(ctx, []byte(path), val)
}

func (s *StoreMessage) SetIf(ctx context.Context, path string, msg protoreflect.ProtoMessage, pred protoreflect.ProtoMessage) error {
	curr, err := s.Store.Get(ctx, []byte(path))
	if err != nil {
		return fmt.Errorf("failed on Get. Path (%s): %w", path, err)
	}
	currMsg := msg.ProtoReflect().New().Interface()
	err = proto.Unmarshal(curr, currMsg)
	if err != nil {
		return fmt.Errorf("failed on unmarshal. Path (%s) (%s): %w", path, currMsg, err)
	}
	if !proto.Equal(pred, currMsg) {
		return fmt.Errorf("failed on predicate. Path (%s): %w", path, ErrPredicateFailed)
	}
	val, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed on marshal. Path (%s) (%s): %w", path, msg, err)
	}
	return s.Store.SetIf(ctx, []byte(path), val, curr)
}

func (s *StoreMessage) Delete(ctx context.Context, path string) error {
	return s.Store.Delete(ctx, []byte(path))
}

// TODO(niro): implement Scan when required

func (s *StoreMessage) Close() {
	s.Store.Close()
}
