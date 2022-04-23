package kv_test

import (
	"context"
	"testing"

	"github.com/golang/protobuf/descriptor"

	kv "github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvutil"
)

func TestStoreModel(t *testing.T) {
	ctx := context.Background()
	store, err := kv.Open(ctx, "mem", "")
	if err != nil {
		t.Failed()
	}
	sm := kv.StoreModel{
		Store: store,
		Model: descriptor.MessageDescriptorProto(kvutil.Repository),
	}
}
