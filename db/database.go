package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/georgysavva/scany/sqlscan"
	"github.com/treeverse/lakefs/logging"
)

type TxFunc func(tx Tx) (interface{}, error)

type Database interface {
	io.Closer
	Tx
	Transact(fn TxFunc, opts ...TxOpt) (interface{}, error)
	Metadata() (map[string]string, error)
	Stats() sql.DBStats
	WithContext(ctx context.Context) Database
}

type QueryOptions struct {
	logger logging.Logger
	ctx    context.Context
}

type SqlDatabase struct {
	db           *sql.DB
	queryOptions *QueryOptions
}

func NewSqlDatabase(db *sql.DB) *SqlDatabase {
	return &SqlDatabase{db: db}
}

func (d *SqlDatabase) getLogger() logging.Logger {
	if d.queryOptions != nil {
		return d.queryOptions.logger
	}
	return logging.Default()
}

func (d *SqlDatabase) getContext() context.Context {
	if d.queryOptions != nil {
		return d.queryOptions.ctx
	}
	return context.Background()
}

func (d *SqlDatabase) WithContext(ctx context.Context) Database {
	return &SqlDatabase{
		db: d.db,
		queryOptions: &QueryOptions{
			logger: logging.Default().WithContext(ctx),
			ctx:    ctx,
		},
	}
}

func (d *SqlDatabase) Close() error {
	return d.db.Close()
}

// performAndReport performs fn and logs a "done" report if its duration was long enough.
func (d *SqlDatabase) performAndReport(fields logging.Fields, fn func() (interface{}, error)) (interface{}, error) {
	start := time.Now()
	ret, err := fn()
	duration := time.Since(start)
	if duration > 100*time.Millisecond {
		logger := d.getLogger().WithFields(fields).WithField("duration", duration)
		if err != nil {
			logger = logger.WithError(err)
		}
		logger.Info("database done")
	}
	return ret, err
}

func (d *SqlDatabase) GetStruct(dest interface{}, query string, args ...interface{}) error {
	_, err := d.performAndReport(logging.Fields{
		"type":  "get",
		"query": query,
		"args":  args,
	}, func() (interface{}, error) {
		return nil, sqlscan.Get(d.getContext(), d.db, dest, query, args...)
	})
	return err
}

func (d *SqlDatabase) Get(dest interface{}, query string, args ...interface{}) error {
	row := d.db.QueryRowContext(context.Background(), query, args...)
	err := row.Scan(dest)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("query %s: %w", query, err)
	}
	return nil
}
func (d *SqlDatabase) Query(query string, args ...interface{}) (rows *sql.Rows, err error) {
	ret, err := d.performAndReport(logging.Fields{
		"type":  "start query",
		"query": query,
		"args":  args,
	}, func() (interface{}, error) { return d.db.QueryContext(d.getContext(), query, args...) })
	if ret == nil {
		return nil, err
	}
	return ret.(*sql.Rows), err
}

func (d *SqlDatabase) Select(results interface{}, query string, args ...interface{}) error {
	return Select(d, results, query, args...)
}

func (d *SqlDatabase) Exec(query string, args ...interface{}) (sql.Result, error) {
	ret, err := d.performAndReport(logging.Fields{
		"type":  "exec",
		"query": query,
		"args":  args,
	}, func() (interface{}, error) { return d.db.ExecContext(d.getContext(), query, args...) })
	if err != nil {
		return nil, err
	}
	return ret.(sql.Result), nil
}

func (d *SqlDatabase) getTxOptions() *TxOptions {
	options := DefaultTxOptions()
	if d.queryOptions != nil {
		options.logger = d.queryOptions.logger
		options.ctx = d.queryOptions.ctx
	}
	return options
}

func (d *SqlDatabase) Transact(fn TxFunc, opts ...TxOpt) (interface{}, error) {
	options := d.getTxOptions()
	for _, opt := range opts {
		opt(options)
	}
	var attempt int
	var ret interface{}
	for attempt < SerializationRetryMaxAttempts {
		if attempt > 0 {
			duration := time.Duration(int(SerializationRetryStartInterval) * attempt)
			dbRetriesCount.Inc()
			options.logger.
				WithField("attempt", attempt).
				WithField("sleep_interval", duration).
				Warn("retrying transaction due to serialization error")
			time.Sleep(duration)
		}

		tx, err := d.db.BeginTx(options.ctx, &sql.TxOptions{
			Isolation: options.isolationLevel,
			ReadOnly:  options.readOnly,
		})
		if err != nil {
			return nil, err
		}
		ret, err = fn(&dbTx{tx: tx, logger: options.logger})
		if err != nil {
			rollbackErr := tx.Rollback()
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
			err = tx.Commit()
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

func (d *SqlDatabase) Metadata() (map[string]string, error) {
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

func (d *SqlDatabase) getVersion() (string, error) {
	v, err := d.Transact(func(tx Tx) (interface{}, error) {
		type ver struct {
			Version string `db:"version"`
		}
		var v ver
		err := tx.GetStruct(&v, "SELECT version()")
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

func (d *SqlDatabase) getAuroraVersion() (string, error) {
	v, err := d.Transact(func(tx Tx) (interface{}, error) {
		var v string
		err := tx.GetStruct(&v, "SELECT aurora_version()")
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

func (d *SqlDatabase) Stats() sql.DBStats {
	return d.db.Stats()
}
