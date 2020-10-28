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
func BuildDatabaseConnection(dbParams params.Database) Database {
	database, err := ConnectDB(dbParams)
	if err != nil {
		panic(err)
	}
	return database
}

func ConnectDB(p params.Database) (Database, error) {
	normalizeDBParams(&p)
	log := logging.Default().WithFields(logging.Fields{
		"driver":               p.Driver,
		"uri":                  p.ConnectionString,
		"max_open_conns":       p.MaxOpenConnections,
		"max_idle_conns":       p.MaxIdleConnections,
		"conn_max_lifetime":    p.ConnectionMaxLifetime,
		"disable_auto_migrate": p.DisableAutoMigrate,
	})
	log.Info("connecting to the DB")
	conn, err := sqlx.Connect(p.Driver, p.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("could not open DB: %w", err)
	}

	conn.SetMaxOpenConns(p.MaxOpenConnections)
	conn.SetMaxIdleConns(p.MaxIdleConnections)
	conn.SetConnMaxLifetime(p.ConnectionMaxLifetime)

	log.Info("initialized DB connection")
	return NewSqlxDatabase(conn), nil
}

func normalizeDBParams(p *params.Database) {
	if p.Driver == "" {
		p.Driver = DatabaseDriver
	}

	if p.MaxOpenConnections == 0 {
		p.MaxOpenConnections = DefaultMaxOpenConnections
	}

	if p.MaxIdleConnections == 0 {
		p.MaxIdleConnections = DefaultMaxIdleConnections
	}

	if p.ConnectionMaxLifetime == 0 {
		p.ConnectionMaxLifetime = DefaultConnectionMaxLifetime
	}
}
