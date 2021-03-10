package db_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/treeverse/lakefs/pkg/db"

	"github.com/ory/dockertest/v3"
)

const (
	dbContainerTimeoutSeconds = 10 * 60 // 10 min
	dbName                    = "lakefs_db"
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
		log.Fatalf("Could not start postgresql: %s", err)
	}

	// set cleanup
	closer := func() {
		err := dockerPool.Purge(resource)
		if err != nil {
			log.Fatalf("could not kill postgres container")
		}
	}

	// expire, just to make sure
	err = resource.Expire(dbContainerTimeoutSeconds)
	if err != nil {
		log.Fatalf("could not expire postgres container")
	}

	// create connection
	var pgPool *pgxpool.Pool
	uri := fmt.Sprintf("postgres://lakefs:lakefs@localhost:%s/"+dbName+"?sslmode=disable", resource.GetPort("5432/tcp"))
	err = dockerPool.Retry(func() error {
		var err error
		pgPool, err = pgxpool.Connect(ctx, uri)
		if err != nil {
			return err
		}
		return db.Ping(ctx, pgPool)
	})
	if err != nil {
		log.Fatalf("could not connect to postgres: %s", err)
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
