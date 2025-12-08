package kv_test

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"path"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/kv"
	_ "github.com/treeverse/lakefs/pkg/kv/dynamodb"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/kv/local"
	"github.com/treeverse/lakefs/pkg/kv/mem"
	_ "github.com/treeverse/lakefs/pkg/kv/mem"
	"github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/testutil"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const modelSetPartitionKey = "tm:set"
const modelGetPartitionKey = "tm:get"

const randomGetSeed int64 = 3_141_593

var testTime = time.Now().UTC()

func TestMsgFuncs(t *testing.T) {
	ctx := context.Background()
	store := kvtest.GetStore(ctx, t)

	t.Run("set-get", func(t *testing.T) {
		testSetGetMsg(t, ctx, store)
	})
	t.Run("set-if", func(t *testing.T) {
		testSetIfMsg(t, ctx, store)
	})
}

func testSetGetMsg(t testing.TB, ctx context.Context, store kv.Store) {
	// set model info
	setModel := &kvtest.TestModel{
		Name:        []byte("SetGetModel"),
		JustAString: "This is another string",
		ADouble:     2.4,
		TestTime:    timestamppb.New(testTime),
		TestMap: map[string]int32{
			"one":   1,
			"two":   2,
			"three": 3,
		},
		TestList: []bool{true, true, false, true, false},
	}
	err := kv.SetMsg(ctx, store, modelSetPartitionKey, setModel.Name, setModel)
	if err != nil {
		t.Fatal("failed to set model", err)
	}

	// get model info
	m := &kvtest.TestModel{}
	_, err = kv.GetMsg(ctx, store, modelSetPartitionKey, setModel.Name, m)
	if err != nil {
		t.Fatal("failed to get message", err)
	}
	equals := proto.Equal(m, setModel)
	if !equals {
		t.Errorf("Get model not equal: %s, expected: %s", m, setModel)
	}
}

func testSetIfMsg(t testing.TB, ctx context.Context, store kv.Store) {
	// set model info
	setModel := &kvtest.TestModel{
		Name:        []byte("SetIfModel"),
		JustAString: "This is another string",
		ADouble:     2.4,
		TestTime:    timestamppb.New(testTime),
		TestMap: map[string]int32{
			"one":   1,
			"two":   2,
			"three": 3,
		},
		TestList: []bool{true, true, false, true, false},
	}
	err := kv.SetMsgIf(ctx, store, modelSetPartitionKey, setModel.Name, setModel, nil)
	if err != nil {
		t.Fatal("failed to set model with nil predicate", err)
	}
	pred, err := kv.GetMsg(ctx, store, modelSetPartitionKey, setModel.Name, nil)
	if err != nil {
		t.Fatal("failed to get model without msg", err)
	}

	// SetIf model
	m1 := &kvtest.TestModel{
		Name:        setModel.Name,
		JustAString: "just another string",
		ADouble:     3.14159,
		TestTime:    timestamppb.New(testTime),
		TestMap: map[string]int32{
			"red":   1,
			"green": 2,
			"blue":  3,
		},
		TestList: []bool{true},
	}

	// SetMsgIf fails nil
	err = kv.SetMsgIf(ctx, store, modelSetPartitionKey, setModel.Name, m1, nil)
	require.Error(t, kv.ErrPredicateFailed, err)

	// get model info
	m2 := &kvtest.TestModel{}
	_, err = kv.GetMsg(ctx, store, modelSetPartitionKey, setModel.Name, m2)
	if err != nil {
		t.Fatal("failed to get message", err)
	}
	equals := proto.Equal(m2, setModel)
	if !equals {
		t.Errorf("Get model not equal: %s, expected: %s", m2, setModel)
	}

	// SetIf succeeds
	err = kv.SetMsgIf(ctx, store, modelSetPartitionKey, setModel.Name, m1, pred)
	if err != nil {
		t.Fatal("failed on SetIf", err)
	}

	_, err = kv.GetMsg(ctx, store, modelSetPartitionKey, setModel.Name, m2)
	if err != nil {
		t.Fatal("failed to get message", err)
	}
	equals = proto.Equal(m2, m1)
	if !equals {
		t.Errorf("Get model not equal: %s, expected: %s", m2, m1)
	}

	// Cleanup
	testutil.MustDo(t, "cleanup", store.Delete(ctx, []byte(modelSetPartitionKey), setModel.Name))
}

func makeKey(i int32) []byte {
	return []byte(fmt.Sprintf("key=%08d", i))
}

func makeGetModel(i int32) *kvtest.TestModel {
	return &kvtest.TestModel{
		Name:        []byte(fmt.Sprintf("model-%08d", i)),
		JustAString: "This is just a string",
		// Use an int, as these have an exact FP representation.
		ADouble:  271828,
		TestTime: timestamppb.New(testTime),
		TestMap: map[string]int32{
			"number": i,
		},
		TestList: []bool{true, true, false, true, false},
	}
}

func setupMsgs(t testing.TB, ctx context.Context, store kv.Store, size int32, parallelism int) {
	start := time.Now()
	t.Logf("Start setup @%s", start)

	var wg errgroup.Group
	wg.SetLimit(parallelism)

	for i := range size {
		key := makeKey(i)
		model := makeGetModel(i)
		wg.Go(func() error {
			return kv.SetMsg(ctx, store, modelGetPartitionKey, key, model)
		})
	}
	err := wg.Wait()
	if err != nil {
		t.Fatal("failed to setup model", err)
	}
	t.Logf("Finished setup in %s", time.Now().Sub(start))
}

func testGetMsgs(t testing.TB, ctx context.Context, store kv.Store, n int, size int32, source rand.Source) {
	var model kvtest.TestModel
	r := rand.New(source)
	for range n {
		index := int32(r.Intn(int(size)))
		key := makeKey(index)
		_, err := kv.GetMsg(ctx, store, modelGetPartitionKey, key, &model)
		if err != nil {
			t.Errorf("Get %s (of %d): %s\n", key, size, err)
			continue
		}
		expected := makeGetModel(index)
		if !proto.Equal(&model, expected) {
			t.Errorf("Index %d got %s expected %s", index, model.String(), expected.String())
		}
	}
}

func BenchmarkDrivers(b *testing.B) {
	ctx := context.Background()

	makeMemStore := func(ctx context.Context, t testing.TB) kv.Store {
		store, err := kv.Open(ctx, kvparams.Config{Type: mem.DriverName})
		if err != nil {
			t.Fatal("failed to open mem store", err)
		}
		return store
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	databaseURI, closer := testutil.GetDBInstance(pool)
	defer closer()

	dynamoStore := testutil.GetDynamoDBProd(ctx, b)
	// Use the same DynamoDB table each time - GetDynamoDBProd will clean it up each time.
	makeDynamoStore := func(ctx context.Context, t testing.TB) kv.Store {
		return dynamoStore
	}

	makePostgresStore := func(ctx context.Context, t testing.TB) kv.Store {
		store, err := kv.Open(ctx, kvparams.Config{
			Type:     postgres.DriverName,
			Postgres: &kvparams.Postgres{ConnectionString: databaseURI},
		})
		if err != nil {
			t.Fatal("failed to open postgres store", err)
		}
		return store
	}

	localKVPath := path.Join(b.TempDir(), "local-kv")
	makeLocalStore := func(ctx context.Context, t testing.TB) kv.Store {
		store, err := kv.Open(ctx, kvparams.Config{
			Type:  local.DriverName,
			Local: &kvparams.Local{Path: localKVPath, PrefetchSize: 256},
		})
		if err != nil {
			t.Fatal("failed to open local store", err)
		}
		return store
	}

	tests := []struct {
		name      string
		makeStore func(context.Context, testing.TB) kv.Store
		// Number of messages to set up to benchmark get.  It should be large enough to discourage
		// caching.  (For local KV, the OS will still cache the pages, which is probably OK if the KV is
		// frequently used!)
		messagesDBSize   int32
		setupParallelism int
	}{
		{
			name:             "mem",
			makeStore:        makeMemStore,
			messagesDBSize:   500_000,
			setupParallelism: 2,
		},
		{
			name:             "local",
			makeStore:        makeLocalStore,
			messagesDBSize:   500_000,
			setupParallelism: 5,
		},
		{
			name:             "postgres",
			makeStore:        makePostgresStore,
			messagesDBSize:   15_000,
			setupParallelism: 2,
		},
		{
			name:             "dynamoDB",
			makeStore:        makeDynamoStore,
			messagesDBSize:   4_000,
			setupParallelism: 50,
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.Run("setget_setif", func(b *testing.B) {
				store := tt.makeStore(ctx, b)
				defer store.Close()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					testSetGetMsg(b, ctx, store)
					testSetIfMsg(b, ctx, store)
				}
			})

			store := tt.makeStore(ctx, b)
			defer func() {
				if store != nil {
					store.Close()
				}
			}()
			setupMsgs(b, ctx, store, tt.messagesDBSize, tt.setupParallelism)

			// If store caches locally, this benchmarks cached data.  kv.Store has
			// not current facility for flushing caches; indeed it is unclear how to
			// do this for the various drivers.

			b.Run("get", func(b *testing.B) {
				source := rand.NewSource(randomGetSeed)
				b.ResetTimer()
				testGetMsgs(b, ctx, store, b.N, tt.messagesDBSize, source)
			})
		})
	}
}
