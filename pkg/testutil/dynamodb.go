package testutil

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/dynamodb"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
)

const (
	dbContainerTimeoutSeconds = 10 * 60 // 10 min
	DynamodbLocalPort         = "6432"
	DynamodbLocalURI          = "http://localhost:6432"
	chars                     = "abcdef1234567890"
	charsSize                 = 8
	DynamoDBScanLimit         = 10
)

func GetDynamoDBInstance() (string, func(), error) {
	dockerPool, err := dockertest.NewPool("")
	if err != nil {
		return "", nil, fmt.Errorf("could not connect to Docker: %w", err)
	}
	dockerPool.MaxWait = dbContainerTimeoutSeconds * time.Second

	dynamodbDockerRunOptions := &dockertest.RunOptions{
		Repository: "amazon/dynamodb-local",
		Tag:        "latest",
		PortBindings: map[dc.Port][]dc.PortBinding{
			"8000/tcp": {{HostPort: DynamodbLocalPort}},
		},
	}

	resource, err := dockerPool.RunWithOptions(dynamodbDockerRunOptions)
	if err != nil {
		return "", nil, fmt.Errorf("could not start dynamodb local: %w", err)
	}

	// set cleanup
	closer := func() {
		err = dockerPool.Purge(resource)
		if err != nil {
			panic("could not kill dynamodb local container")
		}
	}

	// expire, just to make sure
	err = resource.Expire(dbContainerTimeoutSeconds)
	if err != nil {
		return "", nil, fmt.Errorf("could not expire dynamodb local container: %w", err)
	}

	err = dockerPool.Retry(func() error {
		// Waiting for dynamodb container to be ready by issuing an HTTP get request with
		// exponential backoff retry.
		// The response is not really meaningful for that case and so is ignored.
		resp, err := http.Get(DynamodbLocalURI)
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		return nil
	})
	if err != nil {
		return "", nil, fmt.Errorf("could not connect to dynamodb at %s: %w", DynamodbLocalURI, err)
	}

	// return DB URI
	return DynamodbLocalURI, closer, nil
}

func UniqueKVTableName() string {
	return "kvstore_" + UniqueName()
}

func GetDynamoDBProd(ctx context.Context, tb testing.TB) kv.Store {
	testParams := &kvparams.DynamoDB{
		TableName: UniqueKVTableName(),
		ScanLimit: DynamoDBScanLimit,
		AwsRegion: "us-east-1",
	}

	store, err := kv.Open(ctx, kvparams.Config{Type: dynamodb.DriverName, DynamoDB: testParams})
	if err != nil {
		tb.Fatalf("failed to open kv dynamodb store %s", err)
	}
	tb.Cleanup(func() {
		defer store.Close()
		err = store.(*dynamodb.Store).DropTable()
		if err != nil {
			tb.Fatalf("failed to delete table from DB %v %s", table, err)
		}
	})
	return store
}
