package staging

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/hashicorp/go-multierror"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	kvpg "github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/version"
	"google.golang.org/protobuf/proto"
)

const (
	packageName      = "staging"
	jobWorkers       = 10
	migrateQueueSize = 100
)

//nolint:gochecknoinits
func init() {
	kvpg.RegisterMigrate(packageName, Migrate, []string{"graveler_staging_kv"})
}

var encoder kv.SafeEncoder

type DBManager struct {
	db  db.Database
	log logging.Logger
}

func NewDBManager(db db.Database) *DBManager {
	return &DBManager{
		db:  db,
		log: logging.Default().WithField("service_name", "DB_staging_manager"),
	}
}

func (p *DBManager) Get(ctx context.Context, st graveler.StagingToken, key graveler.Key) (*graveler.Value, error) {
	res, err := p.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		value := &graveler.Value{}
		err := tx.Get(value, "SELECT identity, data FROM graveler_staging_kv WHERE staging_token=$1 AND key=$2", st, key)
		return value, err
	}, p.txOpts(db.ReadOnly())...)
	if errors.Is(err, db.ErrNotFound) {
		return nil, graveler.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	value := res.(*graveler.Value)
	if value.Identity == nil {
		return nil, nil
	}
	return value, nil
}

func (p *DBManager) Set(ctx context.Context, st graveler.StagingToken, key graveler.Key, value *graveler.Value, overwrite bool) error {
	if value == nil {
		value = new(graveler.Value)
	} else if value.Identity == nil {
		return graveler.ErrInvalidValue
	}
	_, err := p.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		if !overwrite {
			res, err := tx.Exec(
				`INSERT INTO graveler_staging_kv (staging_token, key, identity, data)
						VALUES ($1, $2, $3, $4)
						ON CONFLICT (staging_token, key) DO NOTHING`,
				st, key, value.Identity, value.Data)
			if err != nil {
				return nil, err
			}
			if res.RowsAffected() == 0 {
				return nil, graveler.ErrPreconditionFailed
			}
			return res, err
		}
		return tx.Exec(`INSERT INTO graveler_staging_kv (staging_token, key, identity, data)
								VALUES ($1, $2, $3, $4)
								ON CONFLICT (staging_token, key) DO UPDATE
									SET (staging_token, key, identity, data) =
											(excluded.staging_token, excluded.key, excluded.identity, excluded.data)`,
			st, key, value.Identity, value.Data)
	}, p.txOpts(db.WithIsolationLevel(pgx.ReadCommitted))...)
	return err
}

func (p *DBManager) Update(_ context.Context, _ graveler.StagingToken, _ graveler.Key, _ graveler.ValueUpdateFunc) error {
	panic("not implemented")
}

func (p *DBManager) DropKey(ctx context.Context, st graveler.StagingToken, key graveler.Key) error {
	_, err := p.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return tx.Exec("DELETE FROM graveler_staging_kv WHERE staging_token=$1 AND key=$2", st, key)
	}, p.txOpts()...)
	return err
}

// List returns an iterator of staged values on the staging token st
func (p *DBManager) List(ctx context.Context, st graveler.StagingToken, batchSize int) (graveler.ValueIterator, error) {
	return NewDBStagingIterator(ctx, p.db, p.log, st, batchSize), nil
}

func (p *DBManager) DropAsync(ctx context.Context, st graveler.StagingToken) error {
	return p.Drop(ctx, st)
}

func (p *DBManager) Drop(ctx context.Context, st graveler.StagingToken) error {
	_, err := p.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return tx.Exec("DELETE FROM graveler_staging_kv WHERE staging_token=$1", st)
	}, p.txOpts()...)
	return err
}

func (p *DBManager) DropByPrefix(ctx context.Context, st graveler.StagingToken, prefix graveler.Key) error {
	upperBound := graveler.UpperBoundForPrefix(prefix)
	builder := sq.Delete("graveler_staging_kv").Where(sq.Eq{"staging_token": st}).Where("key >= ?::bytea", prefix)
	_, err := p.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		if upperBound != nil {
			builder = builder.Where("key < ?::bytea", upperBound)
		}
		query, args, err := builder.PlaceholderFormat(sq.Dollar).ToSql()
		if err != nil {
			return nil, err
		}
		return tx.Exec(query, args...)
	}, p.txOpts()...)
	return err
}

func (p *DBManager) txOpts(opts ...db.TxOpt) []db.TxOpt {
	o := []db.TxOpt{
		db.WithLogger(p.log),
	}
	return append(o, opts...)
}

// runWorker writing staging token data to fd
func runWorker(ctx context.Context, d *pgxpool.Pool, jobChan <-chan string) error {
	record := &graveler.ValueRecord{}
	for t := range jobChan {
		stagingToken := t
		rows, err := d.Query(ctx, "SELECT key, identity, data FROM graveler_staging_kv where staging_token=$1", stagingToken)
		if err != nil {
			return err
		}
		scanner := pgxscan.NewRowScanner(rows)
		for rows.Next() {
			err = scanner.Scan(record)
			if err != nil {
				return err
			}
			pb := graveler.ProtoFromStagedEntry(record.Key, record.Value)
			data, err := proto.Marshal(pb)
			if err != nil {
				return err
			}
			if err = encoder.Encode(kv.Entry{
				PartitionKey: []byte(stagingToken),
				Key:          record.Key,
				Value:        data,
			}); err != nil {
				return err
			}
		}
		rows.Close()
	}
	return nil
}

func Migrate(ctx context.Context, d *pgxpool.Pool, writer io.Writer) error {
	encoder = kv.SafeEncoder{
		Je: json.NewEncoder(writer),
		Mu: sync.Mutex{},
	}
	// Create header
	if err := encoder.Encode(kv.Header{
		LakeFSVersion:   version.Version,
		PackageName:     packageName,
		DBSchemaVersion: kv.InitialMigrateVersion,
		CreatedAt:       time.Now().UTC(),
	}); err != nil {
		return err
	}

	jobChan := make(chan string, migrateQueueSize)
	var g multierror.Group
	for i := 0; i < jobWorkers; i++ {
		// Run worker reads runs from DB per staging token and writes respected entries to fd
		g.Go(func() error {
			return runWorker(ctx, d, jobChan)
		})
	}

	rows, err := d.Query(ctx, "SELECT DISTINCT staging_token FROM graveler_staging_kv")
	if err != nil {
		return err
	}
	defer rows.Close()
	rowScanner := pgxscan.NewRowScanner(rows)
	for rows.Next() {
		m := &struct {
			StagingToken string `db:"staging_token"`
		}{}
		err = rowScanner.Scan(m)
		if err != nil {
			return err
		}

		jobChan <- m.StagingToken
	}
	close(jobChan)
	return g.Wait().ErrorOrNil()
}
