package redis

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/redis/go-redis/v9"
)

const (
	testDBContainerTimeoutSeconds = 10 * 60
	defaultRedisImage             = "redis:7.0-alpine"
)

func testRedisURI() string {
	return os.Getenv("REDIS_TEST_URI")
}

func setupRedis(pool *dockertest.Pool) (func(), string, error) {
	if redisURI := testRedisURI(); redisURI != "" {
		return func() {}, redisURI, nil
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "redis",
		Tag:        "7.0-alpine",
		Env:        []string{},
	}, func(config *dockertest.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = dockertest.RestartPolicy{Name: "no"}
	})
	if err != nil {
		return nil, "", fmt.Errorf("could not start redis: %w", err)
	}

	redisAddr := "localhost:" + resource.GetPort("6379/tcp")

	// Set timeout for docker container
	if err := resource.Expire(testDBContainerTimeoutSeconds); err != nil {
		return nil, "", fmt.Errorf("could not expire redis container: %w", err)
	}

	// Wait for redis to be ready
	pool.MaxWait = 10 * time.Second
	ctx := context.Background()
	if err := pool.Retry(func() error {
		client := redis.NewClient(&redis.Options{
			Addr: redisAddr,
		})
		defer client.Close()
		return client.Ping(ctx).Err()
	}); err != nil {
		return nil, "", fmt.Errorf("could not connect to redis: %w", err)
	}

	return func() {
		if err := pool.Purge(resource); err != nil {
			panic(fmt.Errorf("could not purge redis resource: %w", err))
		}
	}, redisAddr, nil
}