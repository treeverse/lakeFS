package db

import (
	"context"
	"database/sql"
	"regexp"
	"time"

	"github.com/treeverse/lakefs/logging"

	"github.com/jmoiron/sqlx"
)

const (
	SerializationRetryMaxAttempts   = 10
	SerializationRetryStartInterval = time.Millisecond * 2
)

type Tx interface {
	Select(dest interface{}, query string, args ...interface{}) error
	Get(dest interface{}, query string, args ...interface{}) error
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type Database interface {
	Transact(fn TxFunc, opts ...TxOpt) (interface{}, error)
}

type dbTx struct {
	tx     *sqlx.Tx
	logger logging.Logger
}

func queryToString(q string) string {
	r := regexp.MustCompile(`[\t\s\n]+`)
	return r.ReplaceAllString(q, " ")
}

func (d *dbTx) Select(dest interface{}, query string, args ...interface{}) error {
	start := time.Now()
	err := d.tx.Select(dest, query, args...)
	log := d.logger.WithFields(logging.Fields{
		"type":  "select",
		"args":  args,
		"query": queryToString(query),
		"took":  time.Since(start),
	})
	if err != nil {
		log.WithError(err).Error("SQL query failed with error")
		return err
	}
	log.Trace("SQL query executed successfully")
	return nil
}

func (d *dbTx) Get(dest interface{}, query string, args ...interface{}) error {
	start := time.Now()
	err := d.tx.Get(dest, query, args...)
	log := d.logger.WithFields(logging.Fields{
		"type":  "get",
		"args":  args,
		"query": queryToString(query),
		"took":  time.Since(start),
	})
	if err == sql.ErrNoRows {
		log.Trace("SQL query returned no results")
		return ErrNotFound
	}
	if err != nil {
		log.WithError(err).Error("SQL query failed with error")
		return err
	}
	log.Trace("SQL query executed successfully")
	return err
}

func (d *dbTx) Exec(query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	res, err := d.tx.Exec(query, args...)
	log := d.logger.WithFields(logging.Fields{
		"type":  "exec",
		"args":  args,
		"query": queryToString(query),
		"took":  time.Since(start),
	})
	if err != nil {
		log.WithError(err).Error("SQL query failed with error")
		return res, err
	}
	log.Trace("SQL query executed successfully")
	return res, err
}

type SqlxDatabase struct {
	db *sqlx.DB
}

func NewDatabase(db *sqlx.DB) Database {
	return &SqlxDatabase{db: db}
}

type TxFunc func(tx Tx) (interface{}, error)
type TxOpt func(*TxOptions)

type TxOptions struct {
	logger         logging.Logger
	ctx            context.Context
	isolationLevel sql.IsolationLevel
	readOnly       bool
}

func DefaultTxOptions() *TxOptions {
	return &TxOptions{
		logger:         logging.Default(),
		ctx:            context.Background(),
		isolationLevel: sql.LevelSerializable,
		readOnly:       false,
	}
}

func WithLogger(logger logging.Logger) TxOpt {
	return func(o *TxOptions) {
		o.logger = logger
	}
}

func WithContext(ctx context.Context) TxOpt {
	return func(o *TxOptions) {
		o.ctx = ctx
	}
}

func ReadOnly() TxOpt {
	return func(o *TxOptions) {
		o.readOnly = true
	}
}

func WithIsolationLevel(level sql.IsolationLevel) TxOpt {
	return func(o *TxOptions) {
		o.isolationLevel = level
	}
}

func (d *SqlxDatabase) Transact(fn TxFunc, opts ...TxOpt) (interface{}, error) {
	options := DefaultTxOptions()
	for _, opt := range opts {
		opt(options)
	}
	var attempt int
	var ret interface{}
	for attempt < SerializationRetryMaxAttempts {
		if attempt > 0 {
			duration := time.Duration(int(SerializationRetryStartInterval) * attempt)
			options.logger.
				WithField("attempt", attempt).
				WithField("sleep_interval", duration).
				Warn("retrying transaction due to serialization error")
			time.Sleep(duration)
		}

		tx, err := d.db.BeginTxx(options.ctx, &sql.TxOptions{
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
			if isSerializationError(err) {
				// retry
				attempt++
				continue
			}
			return nil, err
		} else {
			err = tx.Commit()
			if err != nil {
				// retry on serialization error
				if isSerializationError(err) {
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

	return nil, ErrSerialization
}
