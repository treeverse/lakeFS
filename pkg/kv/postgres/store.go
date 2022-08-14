package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/treeverse/lakefs/pkg/kv"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
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

	// DefaultPartitions Changing the below value means repartitioning and probably a migration.
	// Change it only if you really know what you're doing.
	DefaultPartitions = 100

	DefaultMaxOpenConnections    = 25
	DefaultMaxIdleConnections    = 25
	DefaultConnectionMaxLifetime = 5 * time.Minute
)

//nolint:gochecknoinits
func init() {
	kv.Register(DriverName, &Driver{})
}

func normalizeDBParams(p *kvparams.Postgres) {
	if p.MaxOpenConnections == 0 {
		p.MaxOpenConnections = DefaultMaxOpenConnections
	}

	if p.MaxIdleConnections == 0 {
		p.MaxIdleConnections = DefaultMaxIdleConnections
	}

	if p.ConnectionMaxLifetime == 0 {
		p.ConnectionMaxLifetime = DefaultConnectionMaxLifetime
	}
}

func (d *Driver) Open(ctx context.Context, kvparams kvparams.KV) (kv.Store, error) {
	// TODO(barak): should we handle Open reuse the same store based on name
	normalizeDBParams(kvparams.Postgres)
	config, err := pgxpool.ParseConfig(kvparams.Postgres.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", kv.ErrDriverConfiguration, err)
	}
	config.MaxConns = kvparams.Postgres.MaxOpenConnections
	config.MinConns = kvparams.Postgres.MaxIdleConnections
	config.MaxConnLifetime = kvparams.Postgres.ConnectionMaxLifetime

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
	PartitionsAmount   int
}

func parseStoreConfig(runtimeParams map[string]string) *Params {
	p := &Params{
		TableName:        DefaultTableName,
		PartitionsAmount: DefaultPartitions,
	}
	if tableName, ok := runtimeParams[paramTableName]; ok {
		p.TableName = tableName
	}
	p.SanitizedTableName = pgx.Identifier{p.TableName}.Sanitize()
	return p
}

// setupKeyValueDatabase setup everything required to enable kv over postgres
func setupKeyValueDatabase(ctx context.Context, conn *pgxpool.Conn, params *Params) error {
	// main kv table
	_, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+params.SanitizedTableName+` (
		partition_key BYTEA NOT NULL,
		key BYTEA NOT NULL,
		value BYTEA NOT NULL,
		UNIQUE (partition_key, key))
	PARTITION BY HASH (partition_key);
	`)
	if err != nil {
		return err
	}

	partitions := getTablePartitions(params.TableName, params.PartitionsAmount)
	for i := 0; i < len(partitions); i++ {
		_, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS`+
			pgx.Identifier{partitions[i]}.Sanitize()+` PARTITION OF `+
			params.SanitizedTableName+` FOR VALUES WITH (MODULUS `+strconv.Itoa(params.PartitionsAmount)+
			`,REMAINDER `+strconv.Itoa(i)+`);`)
		if err != nil {
			return err
		}
	}
	// view of kv table to help humans select from table (same as table with _v as suffix)
	_, err = conn.Exec(ctx, `CREATE OR REPLACE VIEW `+pgx.Identifier{params.TableName + "_v"}.Sanitize()+
		` AS SELECT ENCODE(partition_key, 'escape') AS partition_key, ENCODE(key, 'escape') AS key, value FROM `+params.SanitizedTableName)
	return err
}

func getTablePartitions(tableName string, partitionsAmount int) []string {
	res := make([]string, 0, partitionsAmount)
	for i := 0; i < partitionsAmount; i++ {
		res = append(res, fmt.Sprintf("%s_%d", tableName, i))
	}

	return res
}

func (s *Store) Get(ctx context.Context, partitionKey, key []byte) (*kv.ValueWithPredicate, error) {
	if len(partitionKey) == 0 {
		return nil, kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		return nil, kv.ErrMissingKey
	}

	row := s.Pool.QueryRow(ctx, `SELECT value FROM `+s.Params.SanitizedTableName+` WHERE key = $1 AND partition_key = $2`, key, partitionKey)
	var val []byte
	err := row.Scan(&val)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("key=%v: %w", key, kv.ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w (key=%v)", err, kv.ErrOperationFailed, key)
	}
	return &kv.ValueWithPredicate{
		Value:     val,
		Predicate: kv.Predicate(val),
	}, nil
}

func (s *Store) Set(ctx context.Context, partitionKey, key, value []byte) error {
	if len(partitionKey) == 0 {
		return kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		return kv.ErrMissingKey
	}
	if value == nil {
		return kv.ErrMissingValue
	}

	_, err := s.Pool.Exec(ctx, `INSERT INTO `+s.Params.SanitizedTableName+`(partition_key,key,value) VALUES($1,$2,$3)
			ON CONFLICT (partition_key,key) DO UPDATE SET value = $3`, partitionKey, key, value)
	if err != nil {
		return fmt.Errorf("%s: %w", err, kv.ErrOperationFailed)
	}
	return nil
}

func (s *Store) SetIf(ctx context.Context, partitionKey, key, value []byte, valuePredicate kv.Predicate) error {
	if len(partitionKey) == 0 {
		return kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		return kv.ErrMissingKey
	}
	if value == nil {
		return kv.ErrMissingValue
	}

	var res pgconn.CommandTag
	var err error
	if valuePredicate == nil {
		// use insert to make sure there was no previous value before
		res, err = s.Pool.Exec(ctx, `INSERT INTO `+s.Params.SanitizedTableName+`(partition_key,key,value) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, partitionKey, key, value)
	} else {
		// update just in case the previous value was same as predicate value
		res, err = s.Pool.Exec(ctx, `UPDATE `+s.Params.SanitizedTableName+` SET value=$3 WHERE key=$2 AND partition_key=$1 AND value=$4`, partitionKey, key, value, valuePredicate.([]byte))
	}
	if err != nil {
		return fmt.Errorf("%s: %w (key=%v)", err, kv.ErrOperationFailed, key)
	}
	if res.RowsAffected() != 1 {
		return kv.ErrPredicateFailed
	}
	return nil
}

func (s *Store) Delete(ctx context.Context, partitionKey, key []byte) error {
	if len(partitionKey) == 0 {
		return kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		return kv.ErrMissingKey
	}
	_, err := s.Pool.Exec(ctx, `DELETE FROM `+s.Params.SanitizedTableName+` WHERE partition_key=$1 AND key=$2`, partitionKey, key)
	if err != nil {
		return fmt.Errorf("%s: %w (key=%v)", err, kv.ErrOperationFailed, key)
	}
	return nil
}

func (s *Store) Scan(ctx context.Context, partitionKey, start []byte) (kv.EntriesIterator, error) {
	if len(partitionKey) == 0 {
		return nil, kv.ErrMissingPartitionKey
	}

	var (
		rows pgx.Rows
		err  error
	)

	if start == nil {
		rows, err = s.Pool.Query(ctx, `SELECT partition_key,key,value FROM `+s.Params.SanitizedTableName+` WHERE partition_key=$1 ORDER BY key`, partitionKey)
	} else {
		rows, err = s.Pool.Query(ctx, `SELECT partition_key,key,value FROM `+s.Params.SanitizedTableName+` WHERE partition_key=$1 AND key >= $2 ORDER BY key`, partitionKey, start)
	}

	if err != nil {
		return nil, fmt.Errorf("%s: %w (start=%v)", err, kv.ErrOperationFailed, start)
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
	if err := e.rows.Scan(&ent.PartitionKey, &ent.Key, &ent.Value); err != nil {
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
