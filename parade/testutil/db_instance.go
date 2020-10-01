package testutil

import (
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
	"github.com/ory/dockertest/v3"
)

const (
	dbContainerTimeoutSeconds = 10 * 60 // 10 min
	dbName                    = "parade_db"
)

// RunDBInstance starts a test Postgres server inside container pool, and returns a connection
// URI and a closer function.
func RunDBInstance(pool *dockertest.Pool, postgresUrl string) (string, func()) {
	if postgresUrl != "" {
		return postgresUrl, nil
	}

	resource, err := pool.Run("postgres", "11", []string{
		"POSTGRES_USER=parade",
		"POSTGRES_PASSWORD=parade",
		"POSTGRES_DB=parade_db",
	})
	if err != nil {
		log.Fatalf("could not start postgresql: %s", err)
	}

	// set cleanup
	closer := func() {
		err := pool.Purge(resource)
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
	var conn *sqlx.DB
	uri := fmt.Sprintf("postgres://parade:parade@localhost:%s/"+dbName+"?sslmode=disable", resource.GetPort("5432/tcp"))
	err = pool.Retry(func() error {
		var err error
		conn, err = sqlx.Connect("pgx", uri)
		if err != nil {
			return err
		}
		return conn.Ping()
	})
	if err != nil {
		log.Fatalf("could not connect to postgres: %s", err)
	}

	// Run the DDL
	if _, err = sqlx.LoadFile(conn, "./ddl.sql"); err != nil {
		log.Fatalf("exec command file ./ddl.sql: %s", err)
	}

	_ = conn.Close()

	// return DB URI
	return uri, closer
}
