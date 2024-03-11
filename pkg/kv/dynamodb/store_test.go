package dynamodb_test

import (
	"context"
	"errors"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/dynamodb"
	"github.com/treeverse/lakefs/pkg/testutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
)

var slowdownServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost && r.Header.Get("X-Amz-Target") != "" {
		if r.Header.Get("X-Amz-Target") == "DynamoDB_20120810.PutItem" {
			w.Header().Add("X-Amzn-ErrorType", "RequestLimitExceeded")
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}
	w.WriteHeader(http.StatusOK)
}))

var slowedDownStore = func(t testing.TB, ctx context.Context) kv.Store {
	t.Helper()
	testParams = &kvparams.DynamoDB{
		TableName:          testutil.UniqueKVTableName(),
		ScanLimit:          kvtest.MaxPageSize,
		Endpoint:           slowdownServer.URL,
		AwsRegion:          "us-east-1",
		AwsAccessKeyID:     "fakeMyKeyId",
		AwsSecretAccessKey: "fakeSecretAccessKey",
	}

	store, err := kv.Open(ctx, kvparams.Config{DynamoDB: testParams, Type: dynamodb.DriverName})
	if err != nil {
		t.Fatalf("failed to open kv '%s' store: %s", dynamodb.DriverName, err)
	}
	t.Cleanup(store.Close)
	return store
}

func TestDynamoKV(t *testing.T) {
	driverStore := func(t testing.TB, ctx context.Context) kv.Store {
		t.Helper()
		testParams = &kvparams.DynamoDB{
			TableName:          testutil.UniqueKVTableName(),
			ScanLimit:          kvtest.MaxPageSize,
			Endpoint:           databaseURI,
			AwsRegion:          "us-east-1",
			AwsAccessKeyID:     "fakeMyKeyId",
			AwsSecretAccessKey: "fakeSecretAccessKey",
		}
		store, err := kv.Open(ctx, kvparams.Config{DynamoDB: testParams, Type: dynamodb.DriverName})
		if err != nil {
			t.Fatalf("failed to open kv '%s' store: %s", dynamodb.DriverName, err)
		}
		t.Cleanup(store.Close)
		return store
	}

	kvtest.DriverTestWithExtras(t, driverStore, kvtest.ExtraTest{
		Name: "failure_slowdown",
		Fn: func(t *testing.T) {
			ctx := context.Background()
			store := slowedDownStore(t, ctx)
			defer store.Close()

			key := kvtest.UniqueKey("failure_slowdown")
			val := []byte("v")
			err := store.SetIf(ctx, []byte("test"), key, val, nil)
			if !errors.Is(err, kv.ErrSlowDown) {
				t.Fatalf("SetIf err=%v - expected err=%s", err, kv.ErrSlowDown)
			}
		},
	})
}
