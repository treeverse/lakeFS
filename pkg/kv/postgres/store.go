package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/treeverse/lakefs/pkg/kv"
)

type Driver struct{}

type Store struct {
	Pool           *pgxpool.Pool
	Params         *Params
	TableSanitized string
}

type EntriesIterator struct {
	rows  pgx.Rows
	entry *kv.Entry
	err   error
}

const (
	DriverName = "postgres"

	DefaultTableName = "kv"
	paramTableName   = "lakefskv_table"
)

//nolint:gochecknoinits
func init() {
	kv.Register(DriverName, &Driver{})
}

func (d *Driver) Open(ctx context.Context, name string) (kv.Store, error) {
	// TODO(barak): should we handle Open reuse the same store based on name
	config, err := pgxpool.ParseConfig(name)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", kv.ErrDriverConfiguration, err)
	}
	pool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", kv.ErrConnectFailed, err)
	}
	defer func() {
		// if we return before store uses the pool, free it
		if pool != nil {
			pool.Close()
		}
	}()

	// acquire connection and make sure we reach the database
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", kv.ErrConnectFailed, err)
	}
	defer conn.Release()
	err = conn.Conn().Ping(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", kv.ErrConnectFailed, err)
	}

	params := parseStoreConfig(config.ConnConfig.RuntimeParams)
	err = setupKeyValueDatabase(ctx, conn, params)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", kv.ErrSetupFailed, err)
	}
	store := &Store{
		Pool:           pool,
		Params:         params,
		TableSanitized: pgx.Identifier{params.TableName}.Sanitize(),
	}
	pool = nil
	return store, nil
}

type Params struct {
	TableName          string
	SanitizedTableName string
}

func parseStoreConfig(runtimeParams map[string]string) *Params {
	p := &Params{
		TableName: DefaultTableName,
	}
	if tableName, ok := runtimeParams[paramTableName]; ok {
		p.TableName = tableName
	}
	p.SanitizedTableName = pgx.Identifier{p.TableName}.Sanitize()
	return p
}

// setupKeyValueDatabase setup everything required to enable kv over postgres
func setupKeyValueDatabase(ctx context.Context, conn *pgxpool.Conn, params *Params) error {
	_, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+params.SanitizedTableName+` (
    key BYTEA NOT NULL PRIMARY KEY,
    value BYTEA NOT NULL);`)
	return err
}

func (s *Store) Get(ctx context.Context, key []byte) ([]byte, error) {
	if key == nil {
		return nil, kv.ErrMissingKey
	}
	row := s.Pool.QueryRow(ctx, `SELECT value FROM `+s.Params.SanitizedTableName+` WHERE key = $1`, key)
	var val []byte
	err := row.Scan(&val)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, kv.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w", err, kv.ErrOperationFailed)
	}
	return val, nil
}

func (s *Store) Set(ctx context.Context, key, value []byte) error {
	if key == nil {
		return kv.ErrMissingKey
	}
	if value == nil {
		return kv.ErrMissingValue
	}
	_, err := s.Pool.Exec(ctx, `INSERT INTO `+s.Params.SanitizedTableName+`(key,value) VALUES($1,$2)
			ON CONFLICT (key) DO UPDATE SET value = $2`, key, value)
	if err != nil {
		return fmt.Errorf("%s: %w", err, kv.ErrOperationFailed)
	}
	return nil
}

func (s *Store) SetIf(ctx context.Context, key, value, valuePredicate []byte) error {
	if key == nil {
		return kv.ErrMissingKey
	}
	if value == nil {
		return kv.ErrMissingValue
	}
	var (
		res pgconn.CommandTag
		err error
	)
	if valuePredicate == nil {
		// use insert to make sure there was no previous value before
		res, err = s.Pool.Exec(ctx, `INSERT INTO `+s.Params.SanitizedTableName+`(key,value) VALUES($1,$2) ON CONFLICT DO NOTHING`, key, value)
	} else {
		// update just in case the previous value was same as predicate value
		res, err = s.Pool.Exec(ctx, `UPDATE `+s.Params.SanitizedTableName+` SET value=$2 WHERE key=$1 AND value=$3`, key, value, valuePredicate)
	}
	if err != nil {
		return fmt.Errorf("%s: %w", err, kv.ErrOperationFailed)
	}
	if res.RowsAffected() != 1 {
		return kv.ErrPredicateFailed
	}
	return nil
}

func (s *Store) Delete(ctx context.Context, key []byte) error {
	if key == nil {
		return kv.ErrMissingKey
	}
	_, err := s.Pool.Exec(ctx, `DELETE FROM `+s.Params.SanitizedTableName+` WHERE key=$1`, key)
	if err != nil {
		return fmt.Errorf("%s: %w", err, kv.ErrOperationFailed)
	}
	return nil
}

func (s *Store) Scan(ctx context.Context, start []byte) (kv.EntriesIterator, error) {
	var (
		rows pgx.Rows
		err  error
	)
	if start == nil {
		rows, err = s.Pool.Query(ctx, `SELECT key,value FROM `+s.Params.SanitizedTableName+` ORDER BY key`)
	} else {
		rows, err = s.Pool.Query(ctx, `SELECT key,value FROM `+s.Params.SanitizedTableName+` WHERE key >= $1 ORDER BY key`, start)
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w", err, kv.ErrOperationFailed)
	}
	return &EntriesIterator{
		rows: rows,
	}, nil
}

func (s *Store) Close() {
	s.Pool.Close()
}

// Next reads the next key/value.
func (e *EntriesIterator) Next() bool {
	if e.err != nil {
		return false
	}
	e.entry = nil
	if !e.rows.Next() {
		return false
	}
	var ent kv.Entry
	if err := e.rows.Scan(&ent.Key, &ent.Value); err != nil {
		e.err = fmt.Errorf("%s: %w", err, kv.ErrOperationFailed)
		return false
	}
	e.entry = &ent
	return true
}

func (e *EntriesIterator) Entry() *kv.Entry {
	return e.entry
}

// Err return the last scan error or the cursor error
func (e *EntriesIterator) Err() error {
	if e.err != nil {
		return e.err
	}
	if err := e.rows.Err(); err != nil {
		e.err = fmt.Errorf("%s: %w", err, kv.ErrOperationFailed)
		return err
	}
	return nil
}

func (e *EntriesIterator) Close() {
	e.rows.Close()
	e.entry = nil
	e.err = kv.ErrClosedEntries
}
