package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/treeverse/lakefs/db/params"

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

func Ping(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire to ping: %w", err)
	}
	defer conn.Release()
	err = conn.Conn().Ping(ctx)
	if err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	return nil
}

func ConnectDB(p params.Database) (Database, error) {
	normalizeDBParams(&p)
	log := logging.Default().WithFields(logging.Fields{
		"driver":            p.Driver,
		"uri":               p.ConnectionString,
		"max_open_conns":    p.MaxOpenConnections,
		"max_idle_conns":    p.MaxIdleConnections,
		"conn_max_lifetime": p.ConnectionMaxLifetime,
	})
	log.Info("connecting to the DB")
	config, err := pgxpool.ParseConfig(p.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("parse connection string: %w", err)
	}
	config.MaxConns = p.MaxOpenConnections
	config.MinConns = p.MaxIdleConnections
	config.MaxConnLifetime = p.ConnectionMaxLifetime

	pool, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("could not open DB: %w", err)
	}
	err = Ping(context.Background(), pool)
	if err != nil {
		pool.Close()
		return nil, err
	}

	log.Info("initialized DB connection")
	return NewPgxDatabase(pool), nil
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
