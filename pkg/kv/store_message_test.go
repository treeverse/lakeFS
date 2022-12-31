package kv_test

import (
	"context"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	_ "github.com/treeverse/lakefs/pkg/kv/mem"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	"github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/testutil"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const modelPartitionKey = "tm"

func TestStoreMessage(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)

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

func testStoreMessageSetGet(t testing.TB, ctx context.Context, sm kv.StoreMessage) {
	// set model info
	setModel := &kvtest.TestModel{
		Name:        []byte("SetGetModel"),
		JustAString: "This is another string",
		ADouble:     2.4,
		TestTime:    timestamppb.New(time.Now().UTC()),
		TestMap: map[string]int32{
			"one":   1,
			"two":   2,
			"three": 3,
		},
		TestList: []bool{true, true, false, true, false},
	}
	err := sm.SetMsg(ctx, modelPartitionKey, setModel.Name, setModel)
	if err != nil {
		t.Fatal("failed to set model", err)
	}

	// get model info
	m := &kvtest.TestModel{}
	_, err = sm.GetMsg(ctx, modelPartitionKey, setModel.Name, m)
	if err != nil {
		t.Fatal("failed to get message", err)
	}
	equals := proto.Equal(m, setModel)
	if !equals {
		t.Errorf("Get model not equal: %s, expected: %s", m, setModel)
	}
}

func testStoreMessageSetIf(t testing.TB, ctx context.Context, sm kv.StoreMessage) {
	// set model info
	setModel := &kvtest.TestModel{
		Name:        []byte("SetIfModel"),
		JustAString: "This is another string",
		ADouble:     2.4,
		TestTime:    timestamppb.New(time.Now()),
		TestMap: map[string]int32{
			"one":   1,
			"two":   2,
			"three": 3,
		},
		TestList: []bool{true, true, false, true, false},
	}
	err := sm.SetMsgIf(ctx, modelPartitionKey, setModel.Name, setModel, nil)
	if err != nil {
		t.Fatal("failed to set model with nil predicate", err)
	}
	pred, err := sm.GetMsg(ctx, modelPartitionKey, setModel.Name, nil)
	if err != nil {
		t.Fatal("failed to get model without msg", err)
	}

	// SetIf model
	m1 := &kvtest.TestModel{
		Name:        setModel.Name,
		JustAString: "just another string",
		ADouble:     3.14159,
		TestTime:    timestamppb.New(time.Now().UTC()),
		TestMap: map[string]int32{
			"red":   1,
			"green": 2,
			"blue":  3,
		},
		TestList: []bool{true},
	}

	// SetMsgIf fails nil
	err = sm.SetMsgIf(ctx, modelPartitionKey, setModel.Name, m1, nil)
	require.Error(t, kv.ErrPredicateFailed, err)

	// get model info
	m2 := &kvtest.TestModel{}
	_, err = sm.GetMsg(ctx, modelPartitionKey, setModel.Name, m2)
	if err != nil {
		t.Fatal("failed to get message", err)
	}
	equals := proto.Equal(m2, setModel)
	if !equals {
		t.Errorf("Get model not equal: %s, expected: %s", m2, setModel)
	}

	// SetIf succeeds
	err = sm.SetMsgIf(ctx, modelPartitionKey, setModel.Name, m1, pred)
	if err != nil {
		t.Fatal("failed on SetIf", err)
	}

	_, err = sm.GetMsg(ctx, modelPartitionKey, setModel.Name, m2)
	if err != nil {
		t.Fatal("failed to get message", err)
	}
	equals = proto.Equal(m2, m1)
	if !equals {
		t.Errorf("Get model not equal: %s, expected: %s", m2, m1)
	}

	// Cleanup
	testutil.MustDo(t, "cleanup", sm.DeleteMsg(ctx, modelPartitionKey, setModel.Name))
}

func testStoreMessageDelete(t testing.TB, ctx context.Context, sm kv.StoreMessage) {
	// set model info
	m1 := &kvtest.TestModel{
		Name:        []byte("DeleteModel"),
		JustAString: "This is another string",
		ADouble:     2.4,
		TestTime:    timestamppb.New(time.Now().UTC()),
		TestMap: map[string]int32{
			"one":   1,
			"two":   2,
			"three": 3,
		},
		TestList: []bool{true, true, false, true, false},
	}
	err := sm.SetMsg(ctx, modelPartitionKey, m1.Name, m1)
	if err != nil {
		t.Fatal("failed to set model", err)
	}

	m2 := &kvtest.TestModel{
		Name:        []byte("model2"),
		JustAString: "",
		ADouble:     0,
		TestTime:    nil,
		TestMap:     nil,
		TestList:    nil,
	}
	err = sm.SetMsg(ctx, modelPartitionKey, m2.Name, m2)
	if err != nil {
		t.Fatal("failed to set model", err)
	}

	// delete model2
	err = sm.DeleteMsg(ctx, modelPartitionKey, m2.Name)
	if err != nil {
		t.Fatal("failed to delete message", err)
	}

	// Get deleted key
	m3 := &kvtest.TestModel{}
	_, err = sm.GetMsg(ctx, modelPartitionKey, m2.Name, m3)
	require.Error(t, kv.ErrNotFound, err)

	// delete twice - expect nop
	err = sm.DeleteMsg(ctx, modelPartitionKey, m2.Name)
	if err != nil {
		t.Fatal("error trying to delete non-existing key", err)
	}

	_, err = sm.GetMsg(ctx, modelPartitionKey, m1.Name, m3)
	if err != nil {
		t.Fatal("failed to get message", err)
	}
	equals := proto.Equal(m3, m1)
	if !equals {
		t.Errorf("Get model not equal: %s, expected: %s", m2, m1)
	}

	// delete model1
	err = sm.DeleteMsg(ctx, modelPartitionKey, m1.Name)
	if err != nil {
		t.Fatal("failed to delete message", err)
	}

	// delete twice - expect nop
	err = sm.DeleteMsg(ctx, modelPartitionKey, m1.Name)
	if err != nil {
		t.Fatal("error trying to delete non-existing key", err)
	}

	// Get deleted key (empty Store)
	_, err = sm.GetMsg(ctx, modelPartitionKey, m1.Name, m3)
	require.Error(t, kv.ErrNotFound, err)
}

func testStoreMessageScan(t *testing.T, ctx context.Context, sm kv.StoreMessage) {
	// set model info
	m := &kvtest.TestModel{
		Name:        []byte("ScanModel"),
		JustAString: "This is another string",
		ADouble:     2.4,
		TestTime:    timestamppb.New(time.Now().UTC()),
		TestMap: map[string]int32{
			"one":   1,
			"two":   2,
			"three": 3,
		},
		TestList: []bool{true, true, false, true, false},
	}
	modelKeyPrefix := []byte("m")
	modelNum := 5

	var testData []testItem
	// Add test models to Store
	for i := 0; i < modelNum; i++ {
		msgNew := proto.Clone(m).(*kvtest.TestModel)
		msgNew.TestMap["special"] = int32(i)
		key := kv.FormatPath(string(modelKeyPrefix), strconv.Itoa(i))
		testData = append(testData, testItem{[]byte(key), msgNew})
		require.NoError(t, sm.SetMsg(ctx, modelPartitionKey, []byte(key), msgNew))
	}

	preModelKey := "l"
	preModelData := "This is pre test model"
	require.NoError(t, sm.Store.Set(ctx, []byte(modelPartitionKey), []byte(preModelKey), []byte(preModelData)))
	postModelKey := "n"
	postModelData := "This is post test model"
	require.NoError(t, sm.Store.Set(ctx, []byte(modelPartitionKey), []byte(postModelKey), []byte(postModelData)))

	t.Run("skip just some", func(t *testing.T) {
		skip := 2
		after := testData[skip-1].key

		testScan(t, sm, ctx, testData, modelKeyPrefix, after, skip)
	})
	t.Run("skip not equal", func(t *testing.T) {
		skip := 2
		after := []byte(string(testData[skip-1].key) + "_00000000000")

		testScan(t, sm, ctx, testData, modelKeyPrefix, after, skip)
	})
	t.Run("skip all", func(t *testing.T) {
		skip := 5
		after := testData[skip-1].key

		testScan(t, sm, ctx, testData, modelKeyPrefix, after, skip)
	})
	t.Run("skip none", func(t *testing.T) {
		skip := 0

		testScan(t, sm, ctx, testData, modelKeyPrefix, []byte(""), skip)
	})
}

type testItem struct {
	key []byte
	msg *kvtest.TestModel
}

func testScan(t *testing.T, sm kv.StoreMessage, ctx context.Context, testData []testItem, modelKeyPrefix, after []byte, skip int) {
	itr, err := sm.Scan(ctx, testData[0].msg.ProtoReflect().Type(), modelPartitionKey, modelKeyPrefix, after)
	testutil.MustDo(t, "get iterator", err)
	defer itr.Close()
	count := 0
	for itr.Next() {
		index := count + skip
		entry := itr.Entry()
		require.NotNil(t, entry)
		value, ok := entry.Value.(*kvtest.TestModel)
		require.True(t, ok)
		require.Nil(t, itr.Err())
		require.Equal(t, testData[index].key, entry.Key)
		require.True(t, proto.Equal(value, testData[index].msg))
		count++
	}
	require.Equal(t, len(testData)-skip, count)
}

func testStoreMessageScanWrongFormat(t *testing.T, ctx context.Context, sm kv.StoreMessage) {
	// set model info
	m := &kvtest.TestModel{
		Name:        []byte("DeleteModel"),
		JustAString: "This is another string",
		ADouble:     2.4,
		TestTime:    timestamppb.New(time.Now().UTC()),
		TestMap: map[string]int32{
			"one":   1,
			"two":   2,
			"three": 3,
		},
		TestList: []bool{true, true, false, true, false},
	}
	modelKeyPrefix := "m"
	modelNum := 3

	// Add test models to Store
	for i := 0; i < modelNum; i++ {
		require.NoError(t, sm.SetMsg(ctx, modelPartitionKey, []byte(kv.FormatPath(modelKeyPrefix, strconv.Itoa(i))), m))
	}

	badModelData := "This is a bad model data"
	require.NoError(t, sm.Store.Set(ctx, []byte(modelPartitionKey), []byte(kv.FormatPath(modelKeyPrefix, strconv.Itoa(modelNum))), []byte(badModelData)))

	itr, err := sm.Scan(ctx, m.ProtoReflect().Type(), modelPartitionKey, []byte(modelKeyPrefix), []byte(""))
	testutil.MustDo(t, "get iterator", err)
	defer itr.Close()

	for i := 0; i < modelNum; i++ {
		require.True(t, itr.Next())
		entry := itr.Entry()
		value, ok := entry.Value.(*kvtest.TestModel)
		require.True(t, ok)
		require.Equal(t, []byte(kv.FormatPath(modelKeyPrefix, strconv.Itoa(i))), entry.Key)
		require.True(t, proto.Equal(value, m))
	}

	// bad Entry
	require.False(t, itr.Next())
	value := itr.Entry()
	require.Nil(t, value)
	require.ErrorIs(t, itr.Err(), proto.Error)
	require.False(t, itr.Next())
}

func BenchmarkDrivers(b *testing.B) {
	ctx := context.Background()

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	databaseURI, closer := testutil.GetDBInstance(pool)
	defer closer()

	dynamoStore := testutil.GetDynamoDBProd(ctx, b)
	postgresStore := kvtest.MakeStoreByName(postgres.DriverName, kvparams.Config{Postgres: &kvparams.Postgres{ConnectionString: databaseURI}})(b, ctx)
	defer postgresStore.Close()

	tests := []struct {
		name  string
		store kv.StoreMessage
	}{
		{
			name:  "postgres",
			store: kv.StoreMessage{Store: postgresStore},
		},
		{
			name:  "dynamoDB",
			store: kv.StoreMessage{Store: dynamoStore},
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				testStoreMessageSetGet(b, ctx, tt.store)
				testStoreMessageSetIf(b, ctx, tt.store)
				testStoreMessageDelete(b, ctx, tt.store)
			}
		})
	}
}
