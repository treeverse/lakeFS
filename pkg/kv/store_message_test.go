package kv_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	_ "github.com/treeverse/lakefs/pkg/kv/mem"
	"github.com/treeverse/lakefs/pkg/testutil"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const modelPrefix = "tm"

func TestStoreMessage(t *testing.T) {
	ctx := context.Background()
	store := GetStore(ctx, t)
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
	t.Run("scan test", func(t *testing.T) {
		testStoreMessageScan(t, ctx, sm)
	})
	t.Run("scan wrong format test", func(t *testing.T) {
		testStoreMessageScanWrongFormat(t, ctx, sm)
	})
}

func testStoreMessageSetGet(t *testing.T, ctx context.Context, sm kv.StoreMessage) {
	// set model info
	setModel := &kvtest.TestModel{
		Name:          "SetGetModel",
		AnotherString: "This is another string",
		ADouble:       2.4,
		TestTime:      timestamppb.New(time.Now().UTC()),
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
	_, err = sm.GetMsg(ctx, kv.FormatPath(modelPrefix, setModel.Name), m)
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
	modelPath := kv.FormatPath(modelPrefix, setModel.Name)
	err := sm.SetMsgIf(ctx, modelPath, setModel, nil)
	if err != nil {
		t.Fatal("failed to set model with nil predicate", err)
	}
	pred, err := sm.GetMsg(ctx, modelPath, nil)
	if err != nil {
		t.Fatal("failed to get model without msg", err)
	}

	// SetIf model
	m1 := &kvtest.TestModel{
		Name:          setModel.Name,
		AnotherString: "just another string",
		ADouble:       3.14159,
		TestTime:      timestamppb.New(time.Now().UTC()),
		TestMap: map[string]int32{
			"red":   1,
			"green": 2,
			"blue":  3,
		},
		TestList: []bool{true},
	}

	// SetMsgIf fails nil
	err = sm.SetMsgIf(ctx, modelPath, m1, nil)
	require.Error(t, kv.ErrPredicateFailed, err)

	// get model info
	m2 := &kvtest.TestModel{}
	_, err = sm.GetMsg(ctx, modelPath, m2)
	if err != nil {
		t.Fatal("failed to get message", err)
	}
	equals := proto.Equal(m2, setModel)
	if !equals {
		t.Errorf("Get model not equal: %s, expected: %s", m2, setModel)
	}

	// SetIf succeeds
	err = sm.SetMsgIf(ctx, modelPath, m1, pred)
	if err != nil {
		t.Fatal("failed on SetIf", err)
	}

	_, err = sm.GetMsg(ctx, modelPath, m2)
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
		TestTime:      timestamppb.New(time.Now().UTC()),
		TestMap: map[string]int32{
			"one":   1,
			"two":   2,
			"three": 3,
		},
		TestList: []bool{true, true, false, true, false},
	}
	m1Path := kv.FormatPath(m1.Name)
	err := sm.SetMsg(ctx, m1Path, m1)
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
	err = sm.DeleteMsg(ctx, kv.FormatPath(m2.Name))
	if err != nil {
		t.Fatal("failed to delete message", err)
	}

	// Get deleted key
	m3 := &kvtest.TestModel{}
	_, err = sm.GetMsg(ctx, kv.FormatPath(m2.Name), m3)
	require.Error(t, kv.ErrNotFound, err)

	// delete twice - expect nop
	err = sm.DeleteMsg(ctx, kv.FormatPath(m2.Name))
	if err != nil {
		t.Fatal("error trying to delete non-existing key", err)
	}

	_, err = sm.GetMsg(ctx, m1Path, m3)
	if err != nil {
		t.Fatal("failed to get message", err)
	}
	equals := proto.Equal(m3, m1)
	if !equals {
		t.Errorf("Get model not equal: %s, expected: %s", m2, m1)
	}

	// delete model1
	err = sm.DeleteMsg(ctx, m1Path)
	if err != nil {
		t.Fatal("failed to delete message", err)
	}

	// delete twice - expect nop
	err = sm.DeleteMsg(ctx, m1Path)
	if err != nil {
		t.Fatal("error trying to delete non-existing key", err)
	}

	// Get deleted key (empty store)
	_, err = sm.GetMsg(ctx, m1Path, m3)
	require.Error(t, kv.ErrNotFound, err)
}

func testStoreMessageScan(t *testing.T, ctx context.Context, sm kv.StoreMessage) {
	// set model info
	m := &kvtest.TestModel{
		Name:          "DeleteModel",
		AnotherString: "This is another string",
		ADouble:       2.4,
		TestTime:      timestamppb.New(time.Now().UTC()),
		TestMap: map[string]int32{
			"one":   1,
			"two":   2,
			"three": 3,
		},
		TestList: []bool{true, true, false, true, false},
	}
	modelKeyPrefix := "m"
	modelNum := 5

	// Add test models to store
	for i := 0; i < modelNum; i++ {
		require.NoError(t, sm.SetMsg(ctx, kv.FormatPath(modelKeyPrefix, strconv.Itoa(i)), m))
	}

	preModelKey := "l"
	preModelData := "This is pre test model"
	require.NoError(t, sm.Store.Set(ctx, []byte(preModelKey), []byte(preModelData)))
	postModelKey := "n"
	postModelData := "This is post test model"
	require.NoError(t, sm.Store.Set(ctx, []byte(postModelKey), []byte(postModelData)))
	itr, err := sm.Scan(ctx, m.ProtoReflect().Type(), modelKeyPrefix)
	testutil.MustDo(t, "get iterator", err)
	count := 0
	for itr.Next() {
		entry := itr.Entry()
		require.Nil(t, itr.Err())
		require.NotNil(t, entry)
		value, ok := entry.Value.(*kvtest.TestModel)
		require.True(t, ok)
		require.Equal(t, kv.FormatPath(modelKeyPrefix, strconv.Itoa(count)), entry.Key)
		require.True(t, proto.Equal(value, m))
		count++
	}
	require.Equal(t, modelNum, count)
}

func testStoreMessageScanWrongFormat(t *testing.T, ctx context.Context, sm kv.StoreMessage) {
	// set model info
	m := &kvtest.TestModel{
		Name:          "DeleteModel",
		AnotherString: "This is another string",
		ADouble:       2.4,
		TestTime:      timestamppb.New(time.Now().UTC()),
		TestMap: map[string]int32{
			"one":   1,
			"two":   2,
			"three": 3,
		},
		TestList: []bool{true, true, false, true, false},
	}
	modelKeyPrefix := "m"
	modelNum := 3

	// Add test models to store
	for i := 0; i < modelNum; i++ {
		require.NoError(t, sm.SetMsg(ctx, kv.FormatPath(modelKeyPrefix, strconv.Itoa(i)), m))
	}

	badModelData := "This is a bad model data"
	require.NoError(t, sm.Store.Set(ctx, []byte(kv.FormatPath(modelKeyPrefix, strconv.Itoa(modelNum))), []byte(badModelData)))

	itr, err := sm.Scan(ctx, m.ProtoReflect().Type(), modelKeyPrefix)
	testutil.MustDo(t, "get iterator", err)

	for i := 0; i < modelNum; i++ {
		require.True(t, itr.Next())
		entry := itr.Entry()
		require.Nil(t, itr.Err())
		require.NotNil(t, entry)
		value, ok := entry.Value.(*kvtest.TestModel)
		require.True(t, ok)
		require.Equal(t, kv.FormatPath(modelKeyPrefix, strconv.Itoa(i)), entry.Key)
		require.True(t, proto.Equal(value, m))
	}

	require.True(t, itr.Next())
	badEntry := itr.Entry()
	require.Nil(t, badEntry)
	require.ErrorIs(t, itr.Err(), proto.Error)
	require.False(t, itr.Next())
}

// GetStore helper function to return Store object for all unit tests
func GetStore(ctx context.Context, t *testing.T) kv.Store {
	t.Helper()
	const storeType = "mem"
	store, err := kv.Open(ctx, storeType, "")
	if err != nil {
		t.Fatalf("failed to open kv (%s) store: %s", storeType, err)
	}
	return store
}
