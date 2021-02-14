package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/treeverse/lakefs/logging"
)

type TxFunc func(tx Tx) (interface{}, error)

type Database interface {
	Tx
	Close()
	Transact(fn TxFunc, opts ...TxOpt) (interface{}, error)
	Metadata() (map[string]string, error)
	Stats() sql.DBStats
	WithContext(ctx context.Context) Database
	Pool() *pgxpool.Pool
}

// Void wraps a procedure with no return value as a TxFunc
func Void(fn func(tx Tx) error) TxFunc {
	return func(tx Tx) (interface{}, error) { return nil, fn(tx) }
}

type QueryOptions struct {
	logger logging.Logger
	ctx    context.Context
}

type PgxDatabase struct {
	db           *pgxpool.Pool
	queryOptions *QueryOptions
}

func NewPgxDatabase(db *pgxpool.Pool) *PgxDatabase {
	return &PgxDatabase{db: db}
}

func (d *PgxDatabase) getLogger() logging.Logger {
	if d.queryOptions != nil {
		return d.queryOptions.logger
	}
	return logging.Default()
}

func (d *PgxDatabase) getContext() context.Context {
	if d.queryOptions != nil {
		return d.queryOptions.ctx
	}
	return context.Background()
}

func (d *PgxDatabase) WithContext(ctx context.Context) Database {
	return &PgxDatabase{
		db: d.db,
		queryOptions: &QueryOptions{
			logger: logging.Default().WithContext(ctx),
			ctx:    ctx,
		},
	}
}

func (d *PgxDatabase) Close() {
	d.db.Close()
}

func (d *PgxDatabase) Pool() *pgxpool.Pool {
	return d.db
}

// performAndReport performs fn and logs a "done" report if its duration was long enough.
func (d *PgxDatabase) performAndReport(fields logging.Fields, fn func() (interface{}, error)) (interface{}, error) {
	start := time.Now()
	ret, err := fn()
	duration := time.Since(start)
	if duration > 100*time.Millisecond {
		logger := d.getLogger().WithFields(fields).WithField("took", duration)
		if err != nil {
			logger = logger.WithError(err)
		}
		logger.Info("database done")
	}
	return ret, err
}

func (d *PgxDatabase) Get(dest interface{}, query string, args ...interface{}) error {
	_, err := d.performAndReport(logging.Fields{
		"type":  "get",
		"query": query,
		"args":  args,
	}, func() (interface{}, error) {
		return nil, pgxscan.Get(d.getContext(), d.db, dest, query, args...)
	})
	return err
}

func (d *PgxDatabase) GetPrimitive(dest interface{}, query string, args ...interface{}) error {
	row := d.db.QueryRow(context.Background(), query, args...)
	err := row.Scan(dest)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("query %s: %w", query, err)
	}
	return nil
}
func (d *PgxDatabase) Query(query string, args ...interface{}) (rows pgx.Rows, err error) {
	l := d.getLogger().WithFields(logging.Fields{
		"type":  "start query",
		"query": query,
		"args":  args,
	})
	start := time.Now()
	ret, err := d.db.Query(d.getContext(), query, args...)
	if ret == nil {
		return nil, err
	}
	return Logged(ret, start, l), err
}

func (d *PgxDatabase) Select(results interface{}, query string, args ...interface{}) error {
	return Select(d, results, query, args...)
}

func (d *PgxDatabase) Exec(query string, args ...interface{}) (pgconn.CommandTag, error) {
	ret, err := d.performAndReport(logging.Fields{
		"type":  "exec",
		"query": query,
		"args":  args,
	}, func() (interface{}, error) { return d.db.Exec(d.getContext(), query, args...) })
	if err != nil {
		return nil, err
	}
	return ret.(pgconn.CommandTag), nil
}

func (d *PgxDatabase) getTxOptions() *TxOptions {
	options := DefaultTxOptions()
	if d.queryOptions != nil {
		options.logger = d.queryOptions.logger
		options.ctx = d.queryOptions.ctx
	}
	return options
}

func (d *PgxDatabase) Transact(fn TxFunc, opts ...TxOpt) (interface{}, error) {
	options := d.getTxOptions()
	for _, opt := range opts {
		opt(options)
	}
	var attempt int
	var ret interface{}
	var tx pgx.Tx
	defer func() {
		if p := recover(); p != nil && tx != nil {
			_ = tx.Rollback(options.ctx)
			panic(p)
		}
	}()
	for attempt < SerializationRetryMaxAttempts {
		if attempt > 0 {
			duration := SerializationRetryStartInterval * time.Duration(attempt)
			dbRetriesCount.Inc()
			options.logger.
				WithField("attempt", attempt).
				WithField("sleep_interval", duration).
				Warn("retrying transaction due to serialization error")
			time.Sleep(duration)
		}

		tx, err := d.db.BeginTx(options.ctx, pgx.TxOptions{
			IsoLevel:   options.isolationLevel,
			AccessMode: options.accessMode,
		})
		if err != nil {
			return nil, err
		}
		ret, err = fn(&dbTx{tx: tx, logger: options.logger})
		if err != nil {
			rollbackErr := tx.Rollback(options.ctx)
			if rollbackErr != nil {
				return nil, rollbackErr
			}
			// retry on serialization error
			if IsSerializationError(err) {
				// retry
				attempt++
				continue
			}
			return nil, err
		} else {
			err = tx.Commit(options.ctx)
			if err != nil {
				// retry on serialization error
				if IsSerializationError(err) {
					attempt++
					continue
				}
				// other commit error
				return nil, err
			}
			// committed successfully, we're done
			return ret, nil
		}
	}
	if attempt == SerializationRetryMaxAttempts {
		options.logger.
			WithField("attempt", attempt).
			Warn("transaction failed after max attempts due to serialization error")
	}
	return nil, ErrSerialization
}

func (d *PgxDatabase) Metadata() (map[string]string, error) {
	metadata := make(map[string]string)
	version, err := d.getVersion()
	if err == nil {
		metadata["postgresql_version"] = version
	}
	auroraVersion, err := d.getAuroraVersion()
	if err == nil {
		metadata["postgresql_aurora_version"] = auroraVersion
	}

	m, err := d.Transact(func(tx Tx) (interface{}, error) {
		// select name,setting from pg_settings
		// where name in ('data_directory', 'rds.extensions', 'TimeZone', 'work_mem')
		type pgSettings struct {
			Name    string `db:"name"`
			Setting string `db:"setting"`
		}
		rows, err := tx.Query(
			`SELECT name, setting FROM pg_settings
					WHERE name IN ('data_directory', 'rds.extensions', 'TimeZone', 'work_mem')`)
		if err != nil {
			return nil, err
		}
		settings := make(map[string]string)
		for rows.Next() {
			var setting pgSettings
			err = rows.Scan(&setting)
			if err != nil {
				return nil, err
			}
			if setting.Name == "data_directory" {
				isRDS := strings.HasPrefix(setting.Setting, "/rdsdata")
				settings["is_rds"] = strconv.FormatBool(isRDS)
				continue
			}
			settings[setting.Name] = setting.Setting
		}
		return settings, nil
	}, ReadOnly())
	if err != nil {
		return metadata, nil
	}
	// set pgs settings under the metadata with key prefix
	settings := m.(map[string]string)
	for k, v := range settings {
		metadata["postgresql_setting_"+k] = v
	}
	return metadata, nil
}

func (d *PgxDatabase) getVersion() (string, error) {
	v, err := d.Transact(func(tx Tx) (interface{}, error) {
		type ver struct {
			Version string `db:"version"`
		}
		var v ver
		err := tx.Get(&v, "SELECT version()")
		if err != nil {
			return "", err
		}
		return v.Version, nil
	}, ReadOnly(), WithLogger(logging.Dummy()))
	if err != nil {
		return "", err
	}
	return v.(string), err
}

func (d *PgxDatabase) getAuroraVersion() (string, error) {
	v, err := d.Transact(func(tx Tx) (interface{}, error) {
		var v string
		err := tx.Get(&v, "SELECT aurora_version()")
		if err != nil {
			return "", err
		}
		return v, nil
	}, ReadOnly(), WithLogger(logging.Dummy()))
	if err != nil {
		return "", err
	}
	return v.(string), err
}

func (d *PgxDatabase) Stats() sql.DBStats {
	stat := d.db.Stat()
	return sql.DBStats{
		MaxOpenConnections: int(stat.MaxConns()),
		// Includes conns being constructed, so this violates the invariant documented
		// in DBStats that OpenConnections = InUse + Idle.
		OpenConnections: int(stat.TotalConns()),
		InUse:           int(stat.AcquiredConns()),
		Idle:            int(stat.IdleConns()),
		// WaitCount is the number of connections for which sql had to wait.
		// EmptyAcquireCount is the number of connections that pgx acquired after
		// waiting.  The two are close enough (but race each other).
		WaitCount: stat.EmptyAcquireCount(),
		// Time to acquire is close enough to time spent waiting; fudge.
		WaitDuration: stat.AcquireDuration(),
		// Not clear that pgx can report MaxIdleClosed, MaxIdleTimeClosed,
		// MaxLifetimeClosed.
	}
}
