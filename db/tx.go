package db

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/treeverse/lakefs/logging"
)

const (
	SerializationRetryMaxAttempts   = 10
	SerializationRetryStartInterval = time.Millisecond * 2
)

type Tx interface {
	Query(query string, args ...interface{}) (*sqlx.Rows, error)
	Select(dest interface{}, query string, args ...interface{}) error
	Get(dest interface{}, query string, args ...interface{}) error
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type dbTx struct {
	tx     *sqlx.Tx
	logger logging.Logger
}

func queryToString(q string) string {
	return strings.Join(strings.Fields(q), " ")
}

func (d *dbTx) Query(query string, args ...interface{}) (*sqlx.Rows, error) {
	start := time.Now()
	rows, err := d.tx.Queryx(query, args...)
	log := d.logger.WithFields(logging.Fields{
		"type":  "query",
		"args":  args,
		"query": queryToString(query),
		"took":  time.Since(start),
	})
	if err != nil {
		log.WithError(err).Error("SQL query failed with error")
		return nil, err
	}
	log.Trace("SQL query started successfully")
	return rows, nil
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
		dbErrorsCounter.WithLabelValues("select").Inc()
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
	if errors.Is(err, sql.ErrNoRows) {
		log.Trace("SQL query returned no results")
		return ErrNotFound
	}
	if err != nil {
		dbErrorsCounter.WithLabelValues("get").Inc()
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
		dbErrorsCounter.WithLabelValues("exec").Inc()
		log.WithError(err).Error("SQL query failed with error")
		return res, err
	}
	log.Trace("SQL query executed successfully")
	return res, err
}

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
