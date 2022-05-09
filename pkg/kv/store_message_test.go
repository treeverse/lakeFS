package kv_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	_ "github.com/treeverse/lakefs/pkg/kv/mem"
)

const modelPrefix = "tm"

func TestStoreMessage(t *testing.T) {
	ctx := context.Background()
	makeStore := kvtest.MakeStoreByName("mem", "")
	store := makeStore(t, ctx)
	defer store.Close()

	sm := kv.StoreMessage{
		Store: store,
	}
	t.Run("set-get test", func(t *testing.T) {
		testStoreMessageSetGet(t, ctx, sm)
	})
	t.Run("set-if test", func(t *testing.T) {
		testStoreMessageSetIf(t, ctx, sm)
	})
	t.Run("delete test", func(t *testing.T) {
		testStoreMessageDelete(t, ctx, sm)
	})

}

func testStoreMessageSetGet(t *testing.T, ctx context.Context, sm kv.StoreMessage) {
	// set model info
	setModel := &kvtest.TestModel{
		Name:          "SetGetModel",
		AnotherString: "This is another string",
		ADouble:       2.4,
		TestTime:      timestamppb.New(time.Now()),
		TestMap: map[string]int32{
			"one":   1,
			"two":   2,
			"three": 3,
		},
		TestList: []bool{true, true, false, true, false},
	}
	err := sm.SetMsg(ctx, kv.FormatPath(modelPrefix, setModel.Name), setModel)
	if err != nil {
		t.Fatal("failed to set model", err)
	}

	// get model info
	m := &kvtest.TestModel{}
	err = sm.GetMsg(ctx, kv.FormatPath(modelPrefix, setModel.Name), m)
	if err != nil {
		t.Fatal("failed to get message", err)
	}
	equals := proto.Equal(m, setModel)
	if !equals {
		t.Errorf("Get model not equal: %s, expected: %s", m, setModel)
	}
}

func testStoreMessageSetIf(t *testing.T, ctx context.Context, sm kv.StoreMessage) {
	// set model info
	setModel := &kvtest.TestModel{
		Name:          "SetIfModel",
		AnotherString: "This is another string",
		ADouble:       2.4,
		TestTime:      timestamppb.New(time.Now()),
		TestMap: map[string]int32{
			"one":   1,
			"two":   2,
			"three": 3,
		},
		TestList: []bool{true, true, false, true, false},
	}
	err := sm.SetIf(ctx, kv.FormatPath(modelPrefix, setModel.Name), setModel, nil)
	if err != nil {
		t.Fatal("failed to set model with nil predicate", err)
	}

	// SetIf model
	m1 := &kvtest.TestModel{
		Name:          setModel.Name,
		AnotherString: "just another string",
		ADouble:       3.14159,
		TestTime:      timestamppb.New(time.Now()),
		TestMap: map[string]int32{
			"red":   1,
			"green": 2,
			"blue":  3,
		},
		TestList: []bool{true},
	}

	// SetIf fails nil
	err = sm.SetIf(ctx, kv.FormatPath(modelPrefix, setModel.Name), m1, nil)
	require.Error(t, kv.ErrPredicateFailed, err)

	// SetIf fails
	err = sm.SetIf(ctx, kv.FormatPath(modelPrefix, setModel.Name), m1, m1)
	require.Error(t, kv.ErrPredicateFailed, err)

	// get model info
	m2 := &kvtest.TestModel{}
	err = sm.GetMsg(ctx, kv.FormatPath(modelPrefix, setModel.Name), m2)
	if err != nil {
		t.Fatal("failed to get message", err)
	}
	equals := proto.Equal(m2, setModel)
	if !equals {
		t.Errorf("Get model not equal: %s, expected: %s", m2, setModel)
	}

	// SetIf succeeds
	err = sm.SetIf(ctx, kv.FormatPath(modelPrefix, setModel.Name), m1, setModel)
	if err != nil {
		t.Fatal("failed on SetIf", err)
	}

	err = sm.GetMsg(ctx, kv.FormatPath(modelPrefix, setModel.Name), m2)
	if err != nil {
		t.Fatal("failed to get message", err)
	}
	equals = proto.Equal(m2, m1)
	if !equals {
		t.Errorf("Get model not equal: %s, expected: %s", m2, m1)
	}
}

func testStoreMessageDelete(t *testing.T, ctx context.Context, sm kv.StoreMessage) {
	// set model info
	m1 := &kvtest.TestModel{
		Name:          "DeleteModel",
		AnotherString: "This is another string",
		ADouble:       2.4,
		TestTime:      timestamppb.New(time.Now()),
		TestMap: map[string]int32{
			"one":   1,
			"two":   2,
			"three": 3,
		},
		TestList: []bool{true, true, false, true, false},
	}
	err := sm.SetMsg(ctx, kv.FormatPath(m1.Name), m1)
	if err != nil {
		t.Fatal("failed to set model", err)
	}

	m2 := &kvtest.TestModel{
		Name:          "model2",
		AnotherString: "",
		ADouble:       0,
		TestTime:      nil,
		TestMap:       nil,
		TestList:      nil,
	}
	err = sm.SetMsg(ctx, kv.FormatPath(m2.Name), m2)
	if err != nil {
		t.Fatal("failed to set model", err)
	}

	// delete model2
	err = sm.Delete(ctx, kv.FormatPath(m2.Name))
	if err != nil {
		t.Fatal("failed to delete message", err)
	}

	// Get deleted key
	m3 := &kvtest.TestModel{}
	err = sm.GetMsg(ctx, kv.FormatPath(m2.Name), m3)
	require.Error(t, kv.ErrNotFound, err)

	// delete twice - expect nop
	err = sm.Delete(ctx, kv.FormatPath(m2.Name))
	if err != nil {
		t.Fatal("error trying to delete non-existing key", err)
	}

	err = sm.GetMsg(ctx, kv.FormatPath(m1.Name), m3)
	if err != nil {
		t.Fatal("failed to get message", err)
	}
	equals := proto.Equal(m3, m1)
	if !equals {
		t.Errorf("Get model not equal: %s, expected: %s", m2, m1)
	}

	// delete model1
	err = sm.Delete(ctx, kv.FormatPath(m1.Name))
	if err != nil {
		t.Fatal("failed to delete message", err)
	}

	// delete twice - expect nop
	err = sm.Delete(ctx, kv.FormatPath(m1.Name))
	if err != nil {
		t.Fatal("error trying to delete non-existing key", err)
	}

	// Get deleted key (empty store)
	err = sm.GetMsg(ctx, kv.FormatPath(m1.Name), m3)
	require.Error(t, kv.ErrNotFound, err)
}
