package kv

import (
	"google.golang.org/protobuf/reflect/protoreflect"
)

type StoreModel struct {
	Store
	Model protoreflect.ProtoMessage
}
