package testutil

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/ory/dockertest/v3"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/db"

	"github.com/treeverse/lakefs/block"

	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/index/model"
)

const (
	TimeFormat                = "Jan 2 15:04:05 2006 -0700"
	FixtureRoot               = "lakeFsFixtures"
	DBContainerTimeoutSeconds = 60 * 30 // 30 minutes
)

func GetIndexWithRepo(t *testing.T, conn db.Database) (index.Index, *model.Repo) {
	repoCreateDate, _ := time.Parse(TimeFormat, "Apr 7 15:13:13 2005 -0700")
	createIndex := index.NewDBIndex(conn, index.WithTimeGenerator(func() time.Time {
		return repoCreateDate
	}))
	Must(t, createIndex.CreateRepo("example", "s3://example", "master"))
	return index.NewDBIndex(conn), &model.Repo{
		Id:               "example",
		StorageNamespace: "s3://example",
		CreationDate:     repoCreateDate,
		DefaultBranch:    "master",
	}
}

func GetDBInstance(pool *dockertest.Pool) (string, func()) {
	resource, err := pool.Run("postgres", "11", []string{
		"POSTGRES_USER=lakefs",
		"POSTGRES_PASSWORD=lakefs",
		"POSTGRES_DB=lakefs_db",
	})
	if err != nil {
		log.Fatalf("Could not start postgresql: %s", err)
	}

	// set cleanup
	closer := func() {
		err := pool.Purge(resource)
		if err != nil {
			log.Fatalf("could not kill postgres container")
		}
	}

	// expire, just to make sure
	err = resource.Expire(DBContainerTimeoutSeconds)
	if err != nil {
		log.Fatalf("could not expire postgres container")
	}

	// create connection
	var conn *sqlx.DB
	uri := fmt.Sprintf("postgres://lakefs:lakefs@localhost:%s/lakefs_db?sslmode=disable",
		resource.GetPort("5432/tcp"))
	if err = pool.Retry(func() error {
		var err error
		conn, err = sqlx.Connect("pgx", uri)
		if err != nil {
			return err
		}
		return conn.Ping()
	}); err != nil {
		log.Fatalf("could not connect to postgres: %s", err)
	}
	_ = conn.Close()

	// return DB
	return uri, closer
}

func GetDB(t *testing.T, uri, schemaName string) db.Database {
	// generate uuid as schema name
	generatedSchema := fmt.Sprintf("schema_%s",
		strings.ReplaceAll(uuid.New().String(), "-", ""))

	// create connection
	conn, err := sqlx.Connect("pgx", fmt.Sprintf("%s&search_path=%s", uri, generatedSchema))
	if err != nil {
		t.Fatalf("could not connect to PostgreSQL: %s", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
	})

	database := db.NewDatabase(conn)

	// apply DDL
	_, err = database.Transact(func(tx db.Tx) (interface{}, error) {
		_, err := tx.Exec(fmt.Sprintf(`CREATE SCHEMA %s`, generatedSchema))
		if err != nil {
			return nil, err
		}

		// do the actual migration
		return nil, db.MigrateSchemaAll(tx, schemaName)
	})
	if err != nil {
		t.Fatalf("could not create schema: %v", err)
	}

	// return DB
	return database
}

func GetBlockAdapter(t *testing.T) block.Adapter {
	dir := filepath.Join(os.TempDir(), FixtureRoot, fmt.Sprintf("blocks-%s", uuid.Must(uuid.NewUUID()).String()))
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := block.NewLocalFSAdapter(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		err := os.RemoveAll(dir)
		if err != nil {
			t.Fatal(err)
		}
	})
	return adapter
}

func Must(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("error returned for operation: %v", err)
	}
}
