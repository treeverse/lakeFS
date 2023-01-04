package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/IBM/pgxpoolprometheus"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/treeverse/lakefs/pkg/kv"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
)

type Driver struct{}

type Store struct {
	Pool           *pgxpool.Pool
	Params         *Params
	TableSanitized string
	collector      prometheus.Collector
}

type EntriesIterator struct {
	ctx          context.Context
	entries      []kv.Entry
	currEntryIdx int
	err          error
	store        *Store
}

const (
	DriverName = "postgres"

	DefaultTableName = "kv"
	paramTableName   = "lakefskv_table"

	// DefaultPartitions Changing the below value means repartitioning and probably a migration.
	// Change it only if you really know what you're doing.
	DefaultPartitions   = 100
	DefaultScanPageSize = 1000
)

//nolint:gochecknoinits
func init() {
	kv.Register(DriverName, &Driver{})
}

func (d *Driver) Open(ctx context.Context, kvParams kvparams.KV) (kv.Store, error) {
	if kvParams.Postgres == nil {
		return nil, fmt.Errorf("missing %s settings: %w", DriverName, kv.ErrDriverConfiguration)
	}
	config, err := newPgxpoolConfig(kvParams)
	if err != nil {
		return nil, err
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

	params := parseStoreConfig(config.ConnConfig.RuntimeParams, kvParams.Postgres)
	err = setupKeyValueDatabase(ctx, conn, params.TableName, params.PartitionsAmount)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", kv.ErrSetupFailed, err)
	}

	// register collector to publish pgxpool stats as metrics
	var collector prometheus.Collector
	if params.Metrics {
		collector = pgxpoolprometheus.NewCollector(pool, map[string]string{"db_name": params.TableName})
		err := prometheus.Register(collector)
		if err != nil {
			return nil, err
		}
	}

	store := &Store{
		Pool:           pool,
		Params:         params,
		TableSanitized: pgx.Identifier{params.TableName}.Sanitize(),
		collector:      collector,
	}
	pool = nil
	return store, nil
}

func newPgxpoolConfig(kvParams kvparams.KV) (*pgxpool.Config, error) {
	config, err := pgxpool.ParseConfig(kvParams.Postgres.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", kv.ErrDriverConfiguration, err)
	}
	if kvParams.Postgres.MaxOpenConnections > 0 {
		config.MaxConns = kvParams.Postgres.MaxOpenConnections
	}
	if kvParams.Postgres.MaxIdleConnections > 0 {
		config.MinConns = kvParams.Postgres.MaxIdleConnections
	}
	if kvParams.Postgres.ConnectionMaxLifetime > 0 {
		config.MaxConnLifetime = kvParams.Postgres.ConnectionMaxLifetime
	}
	return config, err
}

type Params struct {
	TableName          string
	SanitizedTableName string
	PartitionsAmount   int
	ScanPageSize       int
	Metrics            bool
}

func parseStoreConfig(runtimeParams map[string]string, pgParams *kvparams.Postgres) *Params {
	p := &Params{
		TableName:        DefaultTableName,
		PartitionsAmount: DefaultPartitions,
		ScanPageSize:     DefaultScanPageSize,
		Metrics:          pgParams.Metrics,
	}
	if tableName, ok := runtimeParams[paramTableName]; ok {
		p.TableName = tableName
	}

	p.SanitizedTableName = pgx.Identifier{p.TableName}.Sanitize()
	if pgParams.ScanPageSize > 0 {
		p.ScanPageSize = pgParams.ScanPageSize
	}
	return p
}

// setupKeyValueDatabase setup everything required to enable kv over postgres
func setupKeyValueDatabase(ctx context.Context, conn *pgxpool.Conn, table string, partitionsAmount int) error {
	// main kv table
	tableSanitize := pgx.Identifier{table}.Sanitize()
	_, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+tableSanitize+` (
		partition_key BYTEA NOT NULL,
		key BYTEA NOT NULL,
		value BYTEA NOT NULL,
		PRIMARY KEY (partition_key, key))
	PARTITION BY HASH (partition_key);
	`)
	if err != nil {
		return err
	}

	partitions := getTablePartitions(table, partitionsAmount)
	for i := 0; i < len(partitions); i++ {
		_, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS`+
			pgx.Identifier{partitions[i]}.Sanitize()+` PARTITION OF `+
			tableSanitize+` FOR VALUES WITH (MODULUS `+strconv.Itoa(partitionsAmount)+
			`,REMAINDER `+strconv.Itoa(i)+`);`)
		if err != nil {
			return err
		}
	}
	// view of kv table to help humans select from table (same as table with _v as suffix)
	_, err = conn.Exec(ctx, `CREATE OR REPLACE VIEW `+pgx.Identifier{table + "_v"}.Sanitize()+
		` AS SELECT ENCODE(partition_key, 'escape') AS partition_key, ENCODE(key, 'escape') AS key, value FROM `+tableSanitize)
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
		return nil, kv.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres get: %w", err)
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
		return fmt.Errorf("postgres set: %w", err)
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
	switch {
	case valuePredicate == nil: // use insert to make sure there was no previous value before
		res, err = s.Pool.Exec(ctx, `INSERT INTO `+s.Params.SanitizedTableName+`(partition_key,key,value) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, partitionKey, key, value)

	case valuePredicate == kv.ConditionalExists: // update only if exists
		res, err = s.Pool.Exec(ctx, `UPDATE `+s.Params.SanitizedTableName+` SET value=$3 WHERE key=$2 AND partition_key=$1`, partitionKey, key, value)

	default: // update just in case the previous value was same as predicate value
		res, err = s.Pool.Exec(ctx, `UPDATE `+s.Params.SanitizedTableName+` SET value=$3 WHERE key=$2 AND partition_key=$1 AND value=$4`, partitionKey, key, value, valuePredicate.([]byte))
	}
	if err != nil {
		return fmt.Errorf("postgres setIf: %w", err)
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
		return fmt.Errorf("postgres delete: %w", err)
	}
	return nil
}

func (s *Store) Scan(ctx context.Context, partitionKey []byte, options kv.ScanOptions) (kv.EntriesIterator, error) {
	if len(partitionKey) == 0 {
		return nil, kv.ErrMissingPartitionKey
	}

	return s.scanInternal(ctx, partitionKey, options, true)
}

func (s *Store) scanInternal(ctx context.Context, partitionKey []byte, options kv.ScanOptions, includeStart bool) (*EntriesIterator, error) {
	var (
		rows pgx.Rows
		err  error
	)

	// limit set to the minimum between option's limit and the configured scan page size
	limit := s.Params.ScanPageSize
	if options.BatchSize > 0 && options.BatchSize < limit {
		limit = options.BatchSize
	}

	if options.KeyStart == nil {
		rows, err = s.Pool.Query(ctx, `SELECT partition_key,key,value FROM `+s.Params.SanitizedTableName+` WHERE partition_key=$1 ORDER BY key LIMIT $2`, partitionKey, limit)
	} else {
		compareOp := ">="
		if !includeStart {
			compareOp = ">"
		}
		rows, err = s.Pool.Query(ctx, `SELECT partition_key,key,value FROM `+s.Params.SanitizedTableName+` WHERE partition_key=$1 AND key `+compareOp+` $2 ORDER BY key LIMIT $3`, partitionKey, options.KeyStart, limit)
	}
	if err != nil {
		return nil, fmt.Errorf("postgres scan: %w", err)
	}
	defer rows.Close()

	var entries []kv.Entry
	err = pgxscan.ScanAll(&entries, rows)
	if err != nil {
		return nil, fmt.Errorf("scanning all entries: %w", err)
	}

	return &EntriesIterator{
		ctx:          ctx,
		entries:      entries,
		currEntryIdx: -1,
		store:        s,
	}, nil
}

func (s *Store) Close() {
	if s.collector != nil {
		prometheus.Unregister(s.collector)
		s.collector = nil
	}
	s.Pool.Close()
}

// Next reads the next key/value.
func (e *EntriesIterator) Next() bool {
	if e.err != nil {
		return false
	}

	e.currEntryIdx++
	if e.currEntryIdx == len(e.entries) {
		if e.currEntryIdx == 0 {
			return false
		}
		partitionKey := e.entries[e.currEntryIdx-1].PartitionKey
		key := e.entries[e.currEntryIdx-1].Key
		tmpIter, err := e.store.scanInternal(e.ctx, partitionKey, kv.ScanOptions{KeyStart: key}, false)
		if err != nil {
			e.err = fmt.Errorf("scan paging: %w", err)
			return false
		}
		if len(tmpIter.entries) == 0 {
			return false
		}
		e.entries = tmpIter.entries
		e.currEntryIdx = 0
	}
	return true
}

func (e *EntriesIterator) Entry() *kv.Entry {
	if e.entries == nil {
		return nil
	}
	return &e.entries[e.currEntryIdx]
}

// Err return the last scan error or the cursor error
func (e *EntriesIterator) Err() error {
	return e.err
}

func (e *EntriesIterator) Close() {
	e.entries = nil
	e.currEntryIdx = -1
	e.err = kv.ErrClosedEntries
}
