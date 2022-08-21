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
	"github.com/treeverse/lakefs/pkg/logging"
)

type TxFunc func(tx Tx) (interface{}, error)

type Querier interface {
	Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error)
}

type Database interface {
	Querier
	Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	GetPrimitive(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Exec(ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error)
	Transact(ctx context.Context, fn TxFunc, opts ...TxOpt) (interface{}, error)

	Close()
	Metadata(ctx context.Context) (map[string]string, error)
	Stats() sql.DBStats
	Pool() *pgxpool.Pool
}

// Void wraps a procedure with no return value as a TxFunc
func Void(fn func(tx Tx) error) TxFunc {
	return func(tx Tx) (interface{}, error) { return nil, fn(tx) }
}

type QueryOptions struct {
	logger logging.Logger
}

type PgxDatabase struct {
	db           *pgxpool.Pool
	queryOptions *QueryOptions
}

func NewPgxDatabase(db *pgxpool.Pool) *PgxDatabase {
	return &PgxDatabase{db: db}
}

func (d *PgxDatabase) getLogger(ctx context.Context, fields logging.Fields) logging.Logger {
	var log logging.Logger
	if d.queryOptions != nil {
		log = d.queryOptions.logger
	} else {
		log = logging.Default()
	}
	return log.WithContext(ctx).WithFields(fields)
}

func (d *PgxDatabase) Close() {
	d.db.Close()
}

func (d *PgxDatabase) Pool() *pgxpool.Pool {
	return d.db
}

// performAndReport performs fn and logs a "done" report if its duration was long enough.
func (d *PgxDatabase) performAndReport(ctx context.Context, fields logging.Fields, fn func() (interface{}, error)) (interface{}, error) {
	start := time.Now()
	ret, err := fn()
	duration := time.Since(start)
	if duration > 100*time.Millisecond {
		logger := d.getLogger(ctx, fields).WithField("took", duration)
		if err != nil {
			logger = logger.WithError(err)
		}
		logger.Info("database done")
	}
	return ret, err
}

func (d *PgxDatabase) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	_, err := d.performAndReport(ctx, logging.Fields{
		"type":  "get",
		"query": query,
		"args":  args,
	}, func() (interface{}, error) {
		return nil, pgxscan.Get(ctx, d.db, dest, query, args...)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

func (d *PgxDatabase) GetPrimitive(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	row := d.db.QueryRow(ctx, query, args...)
	err := row.Scan(dest)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("query %s: %w", query, err)
	}
	return nil
}
func (d *PgxDatabase) Query(ctx context.Context, query string, args ...interface{}) (rows pgx.Rows, err error) {
	l := d.getLogger(ctx, logging.Fields{
		"type":  "start query",
		"query": query,
		"args":  args,
	})
	start := time.Now()
	ret, err := d.db.Query(ctx, query, args...)
	if ret == nil {
		return nil, err
	}
	return Logged(ret, start, l), err
}

func (d *PgxDatabase) Select(ctx context.Context, results interface{}, query string, args ...interface{}) error {
	rows, err := d.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	return pgxscan.ScanAll(results, rows)
}

func (d *PgxDatabase) Exec(ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error) {
	ret, err := d.performAndReport(ctx, logging.Fields{
		"type":  "exec",
		"query": query,
		"args":  args,
	}, func() (interface{}, error) { return d.db.Exec(ctx, query, args...) })
	if err != nil {
		return nil, err
	}
	return ret.(pgconn.CommandTag), nil
}

func (d *PgxDatabase) getTxOptions(ctx context.Context) *TxOptions {
	options := DefaultTxOptions(ctx)
	if d.queryOptions != nil {
		options.logger = d.queryOptions.logger
	}
	return options
}

func (d *PgxDatabase) Transact(ctx context.Context, fn TxFunc, opts ...TxOpt) (interface{}, error) {
	options := d.getTxOptions(ctx)
	for _, opt := range opts {
		opt(options)
	}
	var attempt int
	var ret interface{}
	var err error
	var tx pgx.Tx
	defer func() {
		if p := recover(); p != nil && tx != nil {
			_ = tx.Rollback(ctx)
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

		tx, err = d.db.BeginTx(ctx, pgx.TxOptions{
			IsoLevel:   options.isolationLevel,
			AccessMode: options.accessMode,
		})
		if err != nil {
			return nil, err
		}
		ret, err = fn(&dbTx{tx: tx, logger: options.logger, ctx: ctx})
		if err != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil {
				// returning the original error and not the rollbackErr
				// there are cases we use the return value with specific error, so we capture 'ret'
				return ret, err
			}
			// retry on serialization error
			if IsSerializationError(err) {
				// retry
				attempt++
				continue
			}
		} else {
			commitErr := tx.Commit(ctx)
			if commitErr != nil {
				// retry on serialization error
				if IsSerializationError(commitErr) {
					attempt++
					continue
				}
				// other commit error
				return nil, commitErr
			}
			// committed successfully, we're done
		}
		// always return the callback value with or without the error - some cases depends on the data with an error
		return ret, err
	}
	if attempt == SerializationRetryMaxAttempts {
		options.logger.
			WithField("attempt", attempt).
			Warn("transaction failed after max attempts due to serialization error")
	}
	return nil, ErrSerialization
}

func (d *PgxDatabase) Metadata(ctx context.Context) (map[string]string, error) {
	metadata := make(map[string]string)
	version, err := d.getVersion(ctx)
	if err == nil {
		metadata["postgresql_version"] = version
	}
	auroraVersion, err := d.getAuroraVersion()
	if err == nil {
		metadata["postgresql_aurora_version"] = auroraVersion
	}

	m, err := d.Transact(ctx, func(tx Tx) (interface{}, error) {
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

func (d *PgxDatabase) getVersion(ctx context.Context) (string, error) {
	v, err := d.Transact(ctx, func(tx Tx) (interface{}, error) {
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
	ctx := context.Background()
	v, err := d.Transact(ctx, func(tx Tx) (interface{}, error) {
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
