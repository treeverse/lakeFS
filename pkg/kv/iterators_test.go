package kv_test

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/kv/mock"
)

func TestPartitionIterator_ClosedBehaviour(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	store := mock.NewMockStore(ctrl)
	entIt := mock.NewMockEntriesIterator(ctrl)
	entIt.EXPECT().Close().Times(1)
	store.EXPECT().Scan(ctx, gomock.Any(), gomock.Any()).Return(entIt, nil).Times(1)

	it := kv.NewPartitionIterator(ctx, store, (&kvtest.TestModel{}).ProtoReflect().Type(), "partitionKey", 0)
	it.SeekGE([]byte("test"))
	it.Close()

	err := it.Err() // verify we don't crash or call underlying iterator after we Close()
	if err != nil {
		t.Fatal("Err() on closed iterator", err)
	}
}

func TestPartitionIterator_CloseAfterSeekGEFailed(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	store := mock.NewMockStore(ctrl)
	var entIt *mock.MockEntriesIterator
	entItErr := errors.New("failed to scan")
	store.EXPECT().Scan(ctx, gomock.Any(), gomock.Any()).Return(entIt, entItErr).Times(1)

	it := kv.NewPartitionIterator(ctx, store, (&kvtest.TestModel{}).ProtoReflect().Type(), "partitionKey", 0)
	it.SeekGE([]byte("test"))
	it.Close() // verify we don't crash after after SeekGE failed internally with Scan
}
