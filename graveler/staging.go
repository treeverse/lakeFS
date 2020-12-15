package graveler

import (
	"context"
	"math"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

type stagingManager struct {
	db  db.Database
	log logging.Logger
}

func NewStagingManager(db db.Database) StagingManager {
	return &stagingManager{
		db:  db,
		log: logging.Default().WithField("service_name", "postgres_staging_manager"),
	}
}

func (p *stagingManager) Get(ctx context.Context, st StagingToken, key Key) (*Value, error) {
	res, err := p.db.Transact(func(tx db.Tx) (interface{}, error) {
		value := &Value{}
		err := tx.Get(value, "SELECT identity, data FROM graveler_staging_kv WHERE staging_token=$1 AND key=$2", st, key)
		return value, err
	}, p.txOpts(ctx, db.ReadOnly())...)
	if err != nil {
		return nil, err
	}
	value := res.(*Value)
	if value.Identity == nil {
		return nil, nil
	}
	return value, nil
}

func (p *stagingManager) Set(ctx context.Context, st StagingToken, key Key, value *Value) error {
	if value == nil {
		value = new(Value)
	} else if value.Identity == nil {
		return ErrInvalidValue
	}
	_, err := p.db.Transact(func(tx db.Tx) (interface{}, error) {
		return tx.Exec(`INSERT INTO graveler_staging_kv (staging_token, key, identity, data)
								VALUES ($1, $2, $3, $4)
								ON CONFLICT (staging_token, key) DO UPDATE
									SET (staging_token, key, identity, data) =
											(excluded.staging_token, excluded.key, excluded.identity, excluded.data)`,
			st, key, value.Identity, value.Data)
	}, p.txOpts(ctx)...)
	return err
}

func (p *stagingManager) DropKey(ctx context.Context, st StagingToken, key Key) error {
	_, err := p.db.Transact(func(tx db.Tx) (interface{}, error) {
		return tx.Exec("DELETE FROM graveler_staging_kv WHERE staging_token=$1 AND key=$2", st, key)
	}, p.txOpts(ctx)...)
	return err
}

func (p *stagingManager) List(ctx context.Context, st StagingToken) (ValueIterator, error) {
	return NewStagingIterator(ctx, p.db, p.log, st), nil
}

func (p *stagingManager) Drop(ctx context.Context, st StagingToken) error {
	_, err := p.db.Transact(func(tx db.Tx) (interface{}, error) {
		return tx.Exec("DELETE FROM graveler_staging_kv WHERE staging_token=$1", st)
	}, p.txOpts(ctx)...)
	return err
}

func (p *stagingManager) DropByPrefix(ctx context.Context, st StagingToken, prefix Key) error {
	upperBound := getUpperBoundForPrefix(prefix)
	builder := sq.Delete("graveler_staging_kv").Where(sq.Eq{"staging_token": st}).Where("key >= ?::bytea", prefix)
	_, err := p.db.Transact(func(tx db.Tx) (interface{}, error) {
		if upperBound != nil {
			builder = builder.Where("key < ?::bytea", upperBound)
		}
		query, args, err := builder.PlaceholderFormat(sq.Dollar).ToSql()
		if err != nil {
			return nil, err
		}
		return tx.Exec(query, args...)
	}, p.txOpts(ctx)...)
	return err
}

func getUpperBoundForPrefix(prefix Key) Key {
	idx := len(prefix) - 1
	for idx >= 0 && prefix[idx] == math.MaxUint8 {
		idx--
	}
	if idx == -1 {
		return nil
	}
	upperBound := make(Key, idx+1)
	copy(upperBound, prefix[:idx+1])
	upperBound[idx]++
	return upperBound
}

func (p *stagingManager) txOpts(ctx context.Context, opts ...db.TxOpt) []db.TxOpt {
	o := []db.TxOpt{
		db.WithContext(ctx),
		db.WithLogger(p.log),
	}
	return append(o, opts...)
}
