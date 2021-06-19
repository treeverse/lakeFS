package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/treeverse/lakefs/pkg/db/params"
	"github.com/treeverse/lakefs/pkg/logging"
	"gopkg.in/retry.v1"
)

const (
	DefaultMaxOpenConnections    = 25
	DefaultMaxIdleConnections    = 25
	DefaultConnectionMaxLifetime = 5 * time.Minute
	DatabaseDriver               = "pgx"
	firstWait                    = 50 * time.Millisecond
	waitGrowth                   = 1.2
	maxWait                      = 3 * time.Second
)

// BuildDatabaseConnection returns a database connection based on a pool for the configuration
// in c.
func BuildDatabaseConnection(ctx context.Context, dbParams params.Database) Database {
	database, err := ConnectDB(ctx, dbParams)
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

// ConnectDBPool connects to a database using the database params and returns a connection pool
func ConnectDBPool(ctx context.Context, p params.Database) (*pgxpool.Pool, error) {
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

	pool, err := tryConnectConfig(ctx, config, log)
	if err != nil {
		return nil, err
	}

	err = Ping(ctx, pool)
	if err != nil {
		pool.Close()
		return nil, err
	}

	log.Info("initialized DB connection")
	return pool, err
}

// ConnectDB connects to a database using the database params and returns Database
func ConnectDB(ctx context.Context, p params.Database) (Database, error) {
	pool, err := ConnectDBPool(ctx, p)
	if err != nil {
		return nil, err
	}
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

func tryConnectConfig(ctx context.Context, config *pgxpool.Config, log logging.Logger) (*pgxpool.Pool, error) {
	strategy := retry.LimitTime(maxWait,
		retry.Exponential{
			Initial: firstWait,
			Factor:  waitGrowth,
			Jitter:  true,
		},
	)
	var pool *pgxpool.Pool
	var err error
	for a := retry.Start(strategy, nil); a.Next(); {
		pool, err = pgxpool.ConnectConfig(ctx, config)
		if err == nil {
			return pool, nil
		}
		if a.More() {
			log.WithError(err).Info("could not open DB: Trying again")
		}
	}

	return nil, fmt.Errorf("could not open DB: %w", err)
}
