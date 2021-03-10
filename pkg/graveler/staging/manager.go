package staging

import (
	"context"
	"errors"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
)

type Manager struct {
	db  db.Database
	log logging.Logger
}

func NewManager(db db.Database) *Manager {
	return &Manager{
		db:  db,
		log: logging.Default().WithField("service_name", "postgres_staging_manager"),
	}
}

func (p *Manager) Get(ctx context.Context, st graveler.StagingToken, key graveler.Key) (*graveler.Value, error) {
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

func (p *Manager) Set(ctx context.Context, st graveler.StagingToken, key graveler.Key, value *graveler.Value) error {
	if value == nil {
		value = new(graveler.Value)
	} else if value.Identity == nil {
		return graveler.ErrInvalidValue
	}
	_, err := p.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return tx.Exec(`INSERT INTO graveler_staging_kv (staging_token, key, identity, data)
								VALUES ($1, $2, $3, $4)
								ON CONFLICT (staging_token, key) DO UPDATE
									SET (staging_token, key, identity, data) =
											(excluded.staging_token, excluded.key, excluded.identity, excluded.data)`,
			st, key, value.Identity, value.Data)
	}, p.txOpts()...)
	return err
}

func (p *Manager) DropKey(ctx context.Context, st graveler.StagingToken, key graveler.Key) error {
	_, err := p.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return tx.Exec("DELETE FROM graveler_staging_kv WHERE staging_token=$1 AND key=$2", st, key)
	}, p.txOpts()...)
	return err
}

func (p *Manager) List(ctx context.Context, st graveler.StagingToken) (graveler.ValueIterator, error) {
	return NewStagingIterator(ctx, p.db, p.log, st), nil
}

func (p *Manager) Drop(ctx context.Context, st graveler.StagingToken) error {
	_, err := p.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return tx.Exec("DELETE FROM graveler_staging_kv WHERE staging_token=$1", st)
	}, p.txOpts()...)
	return err
}

func (p *Manager) DropByPrefix(ctx context.Context, st graveler.StagingToken, prefix graveler.Key) error {
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

func (p *Manager) txOpts(opts ...db.TxOpt) []db.TxOpt {
	o := []db.TxOpt{
		db.WithLogger(p.log),
	}
	return append(o, opts...)
}
