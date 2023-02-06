package kv

import (
	"context"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

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
