package redis

import (
	"context"
	"os"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/redis/go-redis/v9"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
)

var redisURI string

func TestMain(m *testing.M) {
	ctx := context.Background()
	pool, err := dockertest.NewPool("")
	if err != nil {
		panic("Could not connect to docker: " + err.Error())
	}

	resource, err := pool.Run("redis", "7.0-alpine", []string{})
	if err != nil {
		panic("Could not start resource: " + err.Error())
	}

	redisURI = "localhost:" + resource.GetPort("6379/tcp")

	// Wait for Redis to be ready
	if err := pool.Retry(func() error {
		client := redis.NewClient(&redis.Options{
			Addr: redisURI,
		})
		defer client.Close()
		return client.Ping(ctx).Err()
	}); err != nil {
		panic("Could not connect to Redis: " + err.Error())
	}

	code := m.Run()

	// Clean up
	if err := pool.Purge(resource); err != nil {
		panic("Could not purge resource: " + err.Error())
	}

	os.Exit(code)
}

func TestDriver(t *testing.T) {
	ctx := context.Background()

	params := kvparams.Config{
		Type: DriverName,
		Redis: &kvparams.Redis{
			Address:   redisURI,
			DB:        0,
			KeyPrefix: "test",
		},
	}

	kvStore, err := kv.Open(ctx, params)
	if err != nil {
		t.Fatalf("failed to open KV store: %v", err)
	}
	defer kvStore.Close()

	// Run the standard KV tests
	kvtest.DriverTest(t, kvStore, nil)
}

func TestRedisKeyFormatting(t *testing.T) {
	store := &Store{
		params: &Params{
			KeyPrefix: "lakefs",
		},
	}

	// Test key formatting with prefix
	key := store.formatKey([]byte("partition1"), []byte("key1"))
	expected := "lakefs:partition1:key1"
	if key != expected {
		t.Errorf("expected %s, got %s", expected, key)
	}

	// Test key parsing with prefix
	partitionKey, keyPart, err := store.parseKey("lakefs:partition1:key1")
	if err != nil {
		t.Fatalf("failed to parse key: %v", err)
	}
	if string(partitionKey) != "partition1" {
		t.Errorf("expected partition1, got %s", string(partitionKey))
	}
	if string(keyPart) != "key1" {
		t.Errorf("expected key1, got %s", string(keyPart))
	}
}

func TestRedisKeyFormattingWithoutPrefix(t *testing.T) {
	store := &Store{
		params: &Params{
			KeyPrefix: "",
		},
	}

	// Test key formatting without prefix
	key := store.formatKey([]byte("partition1"), []byte("key1"))
	expected := "partition1:key1"
	if key != expected {
		t.Errorf("expected %s, got %s", expected, key)
	}

	// Test key parsing without prefix
	partitionKey, keyPart, err := store.parseKey("partition1:key1")
	if err != nil {
		t.Fatalf("failed to parse key: %v", err)
	}
	if string(partitionKey) != "partition1" {
		t.Errorf("expected partition1, got %s", string(partitionKey))
	}
	if string(keyPart) != "key1" {
		t.Errorf("expected key1, got %s", string(keyPart))
	}
}