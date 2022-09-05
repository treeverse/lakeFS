package kv_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	kvpg "github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/kv/testdriver"
)

func TestPartitionIterator_CloseWithNil(t *testing.T) { // Reproduce error where returned value is nil value of type implementing interface
	ctx := context.Background()
	store := testdriver.NewTestStore(kvtest.GetStore(ctx, t))
	sm := kv.StoreMessage{
		Store: store,
	}

	setModel := &kvtest.TestModel{
		Name: []byte("CloseModel"),
	}
	err := sm.SetMsg(ctx, modelPartitionKey, setModel.Name, setModel)
	if err != nil {
		t.Fatal("failed to set model", err)
	}

	itr := kv.NewPartitionIterator(ctx, store, (&kvtest.TestModel{}).ProtoReflect().Type(), modelPartitionKey)
	require.NotNil(t, itr)
	itr.Close()

	itr = kv.NewPartitionIterator(ctx, store, (&kvtest.TestModel{}).ProtoReflect().Type(), modelPartitionKey)
	require.NotNil(t, itr)
	// inject nil interface on scan
	var scanCB testdriver.ScanCB = func(_, _ []byte) (kv.EntriesIterator, error) {
		var fail *kvpg.EntriesIterator
		return fail, fmt.Errorf("some kind of error")
	}
	store.SetStoreCallback(testdriver.ScanCallback, scanCB)

	// Calls scan on first attempt
	itr.Next()
	itr.Close()

	itr.SeekGE(setModel.Name) // itr should be nil now
	itr.Close()
}
