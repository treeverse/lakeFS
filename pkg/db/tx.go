package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	SerializationRetryMaxAttempts   = 10
	SerializationRetryStartInterval = time.Millisecond * 2
)

// Tx abstract the pg transaction.
// It is expected to return errors of this package when applicable:
// 1. ErrNotFound - when a specific row was queried
// 2. ErrAlreadyExists - on conflicts when adding an entry
// 3. ErrSerialization - on serialization errors
type Tx interface {
	Query(query string, args ...interface{}) (pgx.Rows, error)
	Select(dest interface{}, query string, args ...interface{}) error
	Get(dest interface{}, query string, args ...interface{}) error
	GetPrimitive(dest interface{}, query string, args ...interface{}) error
	Exec(query string, args ...interface{}) (pgconn.CommandTag, error)
}

type dbTx struct {
	tx     pgx.Tx
	ctx    context.Context
	logger logging.Logger
}

func queryToString(q string) string {
	return strings.Join(strings.Fields(q), " ")
}

func (d *dbTx) Query(query string, args ...interface{}) (pgx.Rows, error) {
	start := time.Now()
	rows, err := d.tx.Query(d.ctx, query, args...)
	log := d.logger.
		WithContext(d.ctx).
		WithFields(logging.Fields{
			"type":  "query",
			"args":  args,
			"query": queryToString(query),
		})
	if err != nil {
		return nil, handleSQLError(err, "query", query, log)
	}
	log.Trace("SQL query started successfully")
	return Logged(rows, start, log), nil
}

func Select(d *dbTx, results interface{}, query string, args ...interface{}) error {
	rows, err := d.Query(query, args...)
	log := d.logger.
		WithContext(d.ctx).
		WithFields(logging.Fields{
			"type":  "select",
			"args":  args,
			"query": queryToString(query),
		})
	if err != nil {
		return handleSQLError(err, "select", query, log)
	}
	defer rows.Close()
	log.Trace("SQL query executed successfully")
	return pgxscan.ScanAll(results, rows)
}

func (d *dbTx) Select(results interface{}, query string, args ...interface{}) error {
	return Select(d, results, query, args...)
}

func (d *dbTx) Get(dest interface{}, query string, args ...interface{}) error {
	start := time.Now()
	err := pgxscan.Get(d.ctx, d.tx, dest, query, args...)
	log := d.logger.WithFields(logging.Fields{
		"type":  "get",
		"args":  args,
		"query": queryToString(query),
		"took":  time.Since(start),
	})
	if err != nil {
		return handleSQLError(err, "get", query, log)
	}
	log.Trace("SQL query executed successfully")
	return nil
}

func (d *dbTx) GetPrimitive(dest interface{}, query string, args ...interface{}) error {
	start := time.Now()
	row := d.tx.QueryRow(d.ctx, query, args...)
	err := row.Scan(dest)
	log := d.logger.WithFields(logging.Fields{
		"type":  "get",
		"args":  args,
		"query": queryToString(query),
		"took":  time.Since(start),
	})
	if err != nil {
		return handleSQLError(err, "get", query, log)
	}
	log.Trace("SQL query executed successfully")
	return nil
}

func (d *dbTx) Exec(query string, args ...interface{}) (pgconn.CommandTag, error) {
	start := time.Now()
	res, err := d.tx.Exec(d.ctx, query, args...)
	log := d.logger.WithFields(logging.Fields{
		"type":  "exec",
		"args":  args,
		"query": queryToString(query),
		"took":  time.Since(start),
	})
	if err != nil {
		return res, handleSQLError(err, "exec", query, log)
	}
	log.Trace("SQL query executed successfully")
	return res, err
}

func handleSQLError(err error, cmdType string, query string, log logging.Logger) error {
	log.WithError(err).Trace("SQL operation error")
	// Each error that is added here should be updated also in controller.go:handleAPIError
	if isUniqueViolation(err) {
		return ErrAlreadyExists
	}
	if pgxscan.NotFound(err) {
		return ErrNotFound
	}

	dbErrorsCounter.WithLabelValues(cmdType).Inc()
	log.WithError(err).Error("SQL operation completed with error")
	return fmt.Errorf("%s: %w", query, err)
}

type TxOpt func(*TxOptions)

type TxOptions struct {
	logger         logging.Logger
	isolationLevel pgx.TxIsoLevel
	accessMode     pgx.TxAccessMode
}

func DefaultTxOptions(ctx context.Context) *TxOptions {
	return &TxOptions{
		logger:         logging.Default().WithContext(ctx),
		isolationLevel: pgx.Serializable,
		accessMode:     pgx.ReadWrite,
	}
}

func WithLogger(logger logging.Logger) TxOpt {
	return func(o *TxOptions) {
		o.logger = logger
	}
}

func ReadOnly() TxOpt {
	return func(o *TxOptions) {
		o.accessMode = pgx.ReadOnly
	}
}

func WithIsolationLevel(level pgx.TxIsoLevel) TxOpt {
	return func(o *TxOptions) {
		o.isolationLevel = level
	}
}
