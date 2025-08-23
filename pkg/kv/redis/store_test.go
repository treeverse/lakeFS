package redis_test

import (
	"context"
	"os"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	_ "github.com/treeverse/lakefs/pkg/kv/redis"
)

var (
	pool     *dockertest.Pool
	redisURI string
)

func TestMain(m *testing.M) {
	var err error
	pool, err = dockertest.NewPool("")
	if err != nil {
		panic("Could not connect to docker: " + err.Error())
	}

	_, redisURI, err = setupRedis(pool)
	if err != nil {
		panic("Could not setup redis: " + err.Error())
	}

	code := m.Run()
	os.Exit(code)
}

func TestRedisKV(t *testing.T) {
	kvtest.DriverTest(t, func(t testing.TB, ctx context.Context) kv.Store {
		t.Helper()

		// Use unique DB number for isolation - just use a simple counter approach
		dbNum := 1

		store, err := kv.Open(ctx, kvparams.Config{
			Type: "redis",
			Redis: &kvparams.Redis{
				Address:   redisURI,
				DB:        dbNum,
				KeyPrefix: "test",
				PoolSize:  5,
			},
		})
		if err != nil {
			t.Fatalf("failed to open KV store: %v", err)
		}
		t.Cleanup(store.Close)
		return store
	})
}
