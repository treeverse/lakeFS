package db

import (
	"fmt"
	"time"

	"github.com/treeverse/lakefs/db/params"

	"github.com/jmoiron/sqlx"
	"github.com/treeverse/lakefs/logging"
)

const (
	DefaultMaxOpenConnections    = 25
	DefaultMaxIdleConnections    = 25
	DefaultConnectionMaxLifetime = 5 * time.Minute
	DatabaseDriver               = "pgx"
)

// BuildDatabaseConnection returns a database connection based on a pool for the configuration
// in c.
func BuildDatabaseConnection(params *params.Database) Database {
	database, err := ConnectDB(DatabaseDriver, params.DatabaseURI)
	if err != nil {
		panic(err)
	}
	return database
}

func ConnectDB(driver string, uri string) (Database, error) {
	log := logging.Default().WithFields(logging.Fields{
		"driver": driver,
		"uri":    uri,
	})

	log.Info("connecting to the DB")
	conn, err := sqlx.Connect(driver, uri)
	if err != nil {
		return nil, fmt.Errorf("could not open DB: %w", err)
	}

	conn.SetMaxOpenConns(DefaultMaxOpenConnections)
	conn.SetMaxIdleConns(DefaultMaxIdleConnections)
	conn.SetConnMaxLifetime(DefaultConnectionMaxLifetime)

	log.Info("initialized DB connection")
	return NewSqlxDatabase(conn), nil
}
