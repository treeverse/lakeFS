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
