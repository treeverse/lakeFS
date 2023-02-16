package kv_test

import (
	"context"
	"log"
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
		TestTime:    timestamppb.New(time.Now().UTC()),
		TestMap: map[string]int32{
			"one":   1,
			"two":   2,
			"three": 3,
		},
		TestList: []bool{true, true, false, true, false},
	}
	err := kv.SetMsg(ctx, store, modelPartitionKey, setModel.Name, setModel)
	if err != nil {
		t.Fatal("failed to set model", err)
	}

	// get model info
	m := &kvtest.TestModel{}
	_, err = kv.GetMsg(ctx, store, modelPartitionKey, setModel.Name, m)
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
		TestTime:    timestamppb.New(time.Now()),
		TestMap: map[string]int32{
			"one":   1,
			"two":   2,
			"three": 3,
		},
		TestList: []bool{true, true, false, true, false},
	}
	err := kv.SetMsgIf(ctx, store, modelPartitionKey, setModel.Name, setModel, nil)
	if err != nil {
		t.Fatal("failed to set model with nil predicate", err)
	}
	pred, err := kv.GetMsg(ctx, store, modelPartitionKey, setModel.Name, nil)
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
	err = kv.SetMsgIf(ctx, store, modelPartitionKey, setModel.Name, m1, nil)
	require.Error(t, kv.ErrPredicateFailed, err)

	// get model info
	m2 := &kvtest.TestModel{}
	_, err = kv.GetMsg(ctx, store, modelPartitionKey, setModel.Name, m2)
	if err != nil {
		t.Fatal("failed to get message", err)
	}
	equals := proto.Equal(m2, setModel)
	if !equals {
		t.Errorf("Get model not equal: %s, expected: %s", m2, setModel)
	}

	// SetIf succeeds
	err = kv.SetMsgIf(ctx, store, modelPartitionKey, setModel.Name, m1, pred)
	if err != nil {
		t.Fatal("failed on SetIf", err)
	}

	_, err = kv.GetMsg(ctx, store, modelPartitionKey, setModel.Name, m2)
	if err != nil {
		t.Fatal("failed to get message", err)
	}
	equals = proto.Equal(m2, m1)
	if !equals {
		t.Errorf("Get model not equal: %s, expected: %s", m2, m1)
	}

	// Cleanup
	testutil.MustDo(t, "cleanup", store.Delete(ctx, []byte(modelPartitionKey), setModel.Name))
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
		store kv.Store
	}{
		{
			name:  "postgres",
			store: postgresStore,
		},
		{
			name:  "dynamoDB",
			store: dynamoStore,
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				testSetGetMsg(b, ctx, tt.store)
				testSetIfMsg(b, ctx, tt.store)
			}
		})
	}
}
