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
	Pool   *pgxpool.Pool
	Params *Params
}

type Entries struct {
	rows  pgx.Rows
	entry *kv.Entry
	err   error
}

const (
	DriverName = "postgres"

	defaultTableName = "kv"
	paramTableName   = "lakefskv_table"
)

//nolint:gochecknoinits
func init() {
	kv.Register(DriverName, &Driver{})
}

func (d *Driver) Open(ctx context.Context, name string) (kv.Store, error) {
	// TODO(barak): name format - we need to accept a connection string to the db + params related to db + params related to kv
	// TODO(barak): handle reuse - open the same kv - reuse the pool
	config, err := pgxpool.ParseConfig(name)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, err
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
		return nil, err
	}
	defer conn.Release()
	err = conn.Conn().Ping(ctx)
	if err != nil {
		return nil, err
	}

	params := parseStoreConfig(config.ConnConfig.RuntimeParams)

	// migrate database
	err = migrateDatabase(ctx, conn, params)
	if err != nil {
		return nil, err
	}
	store := &Store{
		Pool:   pool,
		Params: params,
	}
	pool = nil
	return store, nil
}

type Params struct {
	TableName string
}

func parseStoreConfig(runtimeParams map[string]string) *Params {
	p := &Params{
		TableName: defaultTableName,
	}
	if tableName, ok := runtimeParams[paramTableName]; ok {
		p.TableName = tableName
	}
	return p
}

func migrateDatabase(ctx context.Context, conn *pgxpool.Conn, params *Params) error {
	_, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+params.TableName+` (
    key BYTEA NOT NULL PRIMARY KEY,
    value BYTEA NOT NULL);`)
	return err
}

func (s *Store) Get(ctx context.Context, key []byte) ([]byte, error) {
	if key == nil {
		return nil, kv.ErrMissingKey
	}
	row := s.Pool.QueryRow(ctx, `SELECT value FROM `+s.Params.TableName+` WHERE key = $1`, key)
	var val []byte
	err := row.Scan(&val)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, kv.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %s", kv.ErrOperationFailed, err)
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
	_, err := s.Pool.Exec(ctx, `INSERT INTO `+s.Params.TableName+`(key,value) VALUES($1,$2)
			ON CONFLICT (key) DO UPDATE SET value = $2`, key, value)
	if err != nil {
		return fmt.Errorf("%w: %s", kv.ErrOperationFailed, err)
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
		res, err = s.Pool.Exec(ctx, `INSERT INTO `+s.Params.TableName+`(key,value) VALUES($1,$2) ON CONFLICT DO NOTHING`, key, value)
	} else {
		// update just in case the previous value was same as predicate value
		res, err = s.Pool.Exec(ctx, `UPDATE `+s.Params.TableName+` SET value=$2 WHERE key=$1 AND value=$3`, key, value, valuePredicate)
	}
	if err != nil {
		return fmt.Errorf("%w: %s", kv.ErrOperationFailed, err)
	}
	if res.RowsAffected() != 1 {
		return kv.ErrNotFound
	}
	return nil
}

func (s *Store) Delete(ctx context.Context, key []byte) error {
	if key == nil {
		return kv.ErrMissingKey
	}
	res, err := s.Pool.Exec(ctx, `DELETE FROM `+s.Params.TableName+` WHERE key=$1`, key)
	if err != nil {
		return fmt.Errorf("%w: %s", kv.ErrOperationFailed, err)
	}
	if res.RowsAffected() != 1 {
		return kv.ErrNotFound
	}
	return nil
}

func (s *Store) Scan(ctx context.Context, start []byte) (kv.Entries, error) {
	var (
		rows pgx.Rows
		err  error
	)
	if start == nil {
		rows, err = s.Pool.Query(ctx, `SELECT key,value FROM `+s.Params.TableName+` ORDER BY key`)
	} else {
		rows, err = s.Pool.Query(ctx, `SELECT key,value FROM `+s.Params.TableName+` WHERE key >= $1 ORDER BY key`, start)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %s", kv.ErrOperationFailed, err)
	}
	return &Entries{
		rows: rows,
	}, nil
}

func (s *Store) Close() {
	s.Pool.Close()
}

// Next read the next key/value on any error entry will be nil, err will be set by trying to scan the results
func (e *Entries) Next() bool {
	e.entry = nil
	e.err = nil
	if !e.rows.Next() {
		return false
	}
	var ent kv.Entry
	if err := e.rows.Scan(&ent.Key, &ent.Value); err != nil {
		e.err = fmt.Errorf("%w: %s", kv.ErrOperationFailed, err)
		return false
	}
	e.entry = &ent
	return true
}

func (e *Entries) Entry() *kv.Entry {
	return e.entry
}

// Err return the last scan error or the cursor error
func (e *Entries) Err() error {
	if e.err != nil {
		return e.err
	}
	if err := e.rows.Err(); err != nil {
		e.err = fmt.Errorf("%w: %s", kv.ErrOperationFailed, err)
		return err
	}
	return nil
}

func (e *Entries) Close() {
	e.rows.Close()
	e.entry = nil
	e.err = kv.ErrClosedEntries
}
