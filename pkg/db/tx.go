package db

import (
	"context"
	"errors"
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
		log.WithError(err).Error("SQL query failed with error")
		return nil, err
	}
	log.Trace("SQL query started successfully")
	return Logged(rows, start, log), nil
}

func Select(d Tx, results interface{}, query string, args ...interface{}) error {
	rows, err := d.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
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
	if pgxscan.NotFound(err) {
		// This err comes directly from scany, not directly from pgx, so *must* use
		// pgxscan.NotFound.
		log.Trace("SQL query returned no results")
		return ErrNotFound
	}
	if err != nil {
		dbErrorsCounter.WithLabelValues("get").Inc()
		log.WithError(err).Error("SQL query failed with error")
		return fmt.Errorf("query %s: %w", query, err)
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
	if errors.Is(err, pgx.ErrNoRows) {
		log.Trace("SQL query returned no results")
		return ErrNotFound
	}
	if err != nil {
		dbErrorsCounter.WithLabelValues("get").Inc()
		log.WithError(err).Error("SQL query failed with error")
		return fmt.Errorf("query %s: %w", query, err)
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
	if isUniqueViolation(err) {
		return nil, ErrAlreadyExists
	}
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

func ReadCommitted() TxOpt {
	return func(o *TxOptions) {
		o.isolationLevel = pgx.ReadCommitted
	}
}
func RepeatableRead() TxOpt {
	return func(o *TxOptions) {
		o.isolationLevel = pgx.RepeatableRead
	}
}

func WithIsolationLevel(level pgx.TxIsoLevel) TxOpt {
	return func(o *TxOptions) {
		o.isolationLevel = level
	}
}
