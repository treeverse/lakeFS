package postgres

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
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
	store        *Store
	entries      []kv.Entry
	limit        int
	currEntryIdx int
	err          error
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

func (d *Driver) Open(ctx context.Context, kvParams kvparams.Config) (kv.Store, error) {
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

	// register collector to publish pgx's pool stats as metrics
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

func newPgxpoolConfig(kvParams kvparams.Config) (*pgxpool.Config, error) {
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
func setupKeyValueDatabase(ctx context.Context, conn *pgxpool.Conn, table string, partitionsAmount int) (err error) {
	var aid string
	aid, err = generateAdvisoryLockID("lakefs:" + table)
	if err != nil {
		return err
	}

	// This will wait indefinitely until the lock can be acquired.
	_, err = conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, aid)
	if err != nil {
		return fmt.Errorf("try lock failed: %w", err)
	}
	defer func(ctx context.Context) {
		_, unlockErr := conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, aid)
		// prefer the last error over unlock error
		if err == nil {
			err = unlockErr
		}
	}(ctx)

	// main kv table
	tableSanitize := pgx.Identifier{table}.Sanitize()
	_, err = conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+tableSanitize+` (
		partition_key BYTEA NOT NULL,
		key BYTEA NOT NULL,
		value BYTEA NOT NULL,
		PRIMARY KEY (partition_key, key))
	PARTITION BY HASH (partition_key)`)
	if err != nil {
		return err
	}

	// partitions
	partitions := getTablePartitions(table, partitionsAmount)
	for i := 0; i < len(partitions); i++ {
		_, err = conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS`+
			pgx.Identifier{partitions[i]}.Sanitize()+` PARTITION OF `+
			tableSanitize+` FOR VALUES WITH (MODULUS `+strconv.Itoa(partitionsAmount)+
			`,REMAINDER `+strconv.Itoa(i)+`)`)
		if err != nil {
			return err
		}
	}
	// view of kv table to help humans select from table (same as table with _v as suffix)
	_, err = conn.Exec(ctx, `CREATE OR REPLACE VIEW `+pgx.Identifier{table + "_v"}.Sanitize()+
		` AS SELECT ENCODE(partition_key, 'escape') AS partition_key, ENCODE(key, 'escape') AS key, value FROM `+tableSanitize)
	return err
}

func generateAdvisoryLockID(name string) (string, error) {
	h := fnv.New32a()
	if _, err := h.Write([]byte(name)); err != nil {
		return "", err
	}
	aid := fmt.Sprint(h.Sum32())
	return aid, nil
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
	switch valuePredicate {
	case nil: // use insert to make sure there was no previous value before
		res, err = s.Pool.Exec(ctx, `INSERT INTO `+s.Params.SanitizedTableName+`(partition_key,key,value) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, partitionKey, key, value)

	case kv.PrecondConditionalExists: // update only if exists
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

	// limit based on the minimum between ScanPageSize and ScanOptions batch size
	limit := s.Params.ScanPageSize
	if options.BatchSize != 0 && s.Params.ScanPageSize != 0 && options.BatchSize < s.Params.ScanPageSize {
		limit = options.BatchSize
	}

	entries, err := s.scanInternal(ctx, partitionKey, options.KeyStart, limit, true)
	if err != nil {
		return nil, err
	}
	return &EntriesIterator{
		ctx:          ctx,
		store:        s,
		entries:      entries,
		limit:        limit,
		currEntryIdx: -1,
	}, nil
}

func (s *Store) scanInternal(ctx context.Context, partitionKey []byte, keyStart []byte, limit int, includeStart bool) ([]kv.Entry, error) {
	var (
		rows pgx.Rows
		err  error
	)

	if keyStart == nil {
		rows, err = s.Pool.Query(ctx, `SELECT partition_key,key,value FROM `+s.Params.SanitizedTableName+` WHERE partition_key=$1 ORDER BY key LIMIT $2`, partitionKey, limit)
	} else {
		compareOp := ">="
		if !includeStart {
			compareOp = ">"
		}
		rows, err = s.Pool.Query(ctx, `SELECT partition_key,key,value FROM `+s.Params.SanitizedTableName+` WHERE partition_key=$1 AND key `+compareOp+` $2 ORDER BY key LIMIT $3`, partitionKey, keyStart, limit)
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
	return entries, nil
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
		entries, err := e.store.scanInternal(e.ctx, partitionKey, key, e.limit, false)
		if err != nil {
			e.err = fmt.Errorf("scan paging: %w", err)
			return false
		}
		if len(entries) == 0 {
			return false
		}
		e.entries = entries
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
