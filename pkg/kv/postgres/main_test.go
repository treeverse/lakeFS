package postgres_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/pkg/testutil"
)

const (
	dbContainerTimeoutSeconds = 10 * 60 // 10 min
)

var (
	pool        *dockertest.Pool
	databaseURI string
)

func runDBInstance(dockerPool *dockertest.Pool) (string, func()) {
	ctx := context.Background()
	resource, err := dockerPool.Run("postgres", "11", []string{
		"POSTGRES_USER=lakefs",
		"POSTGRES_PASSWORD=lakefs",
		"POSTGRES_DB=lakefs_db",
	})
	if err != nil {
		panic("Could not start postgresql: " + err.Error())
	}

	// set cleanup
	closer := func() {
		err := dockerPool.Purge(resource)
		if err != nil {
			panic("could not kill postgres container")
		}
	}

	// expire, just to make sure
	err = resource.Expire(dbContainerTimeoutSeconds)
	if err != nil {
		panic("could not expire postgres container")
	}

	// create connection
	var pgPool *pgxpool.Pool
	port := resource.GetPort("5432/tcp")
	uri := fmt.Sprintf("postgres://lakefs:lakefs@localhost:%s/lakefs_db?sslmode=disable", port)
	err = dockerPool.Retry(func() error {
		var err error
		pgPool, err = pgxpool.New(ctx, uri)
		if err != nil {
			return err
		}
		return testutil.PingPG(ctx, pgPool)
	})
	if err != nil {
		panic("could not connect to postgres: " + err.Error())
	}
	pgPool.Close()

	// return DB URI
	return uri, closer
}

func TestMain(m *testing.M) {
	var err error
	pool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	var cleanup func()
	databaseURI, cleanup = runDBInstance(pool)
	code := m.Run()
	cleanup()
	os.Exit(code)
}
