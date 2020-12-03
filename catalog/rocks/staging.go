package rocks

import (
	"context"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

type PGStagingManager struct {
	db  db.Database
	log logging.Logger
}

func NewStageManager(db db.Database) *PGStagingManager {
	return &PGStagingManager{
		db:  db,
		log: logging.Default().WithField("service_name", "postgres_staging_manager"),
	}
}

func (p *PGStagingManager) GetEntry(ctx context.Context, st StagingToken, path Path) (*Entry, error) {
	res, err := p.db.Transact(func(tx db.Tx) (interface{}, error) {
		entry := &Entry{}
		err := tx.Get(entry, "SELECT address, last_modified_date, size, e_tag, metadata FROM staging_entries WHERE staging_token=$1 AND path=$2", st, path)
		return entry, err
	}, p.txOpts(ctx, db.ReadOnly())...)
	if err != nil {
		return nil, err
	}
	entry := res.(*Entry)
	if entry.Address == "" {
		return nil, nil
	}
	return entry, nil
}

func (p *PGStagingManager) SetEntry(ctx context.Context, st StagingToken, path Path, entry *Entry) error {
	if entry == nil {
		entry = &Entry{}
	}
	_, err := p.db.Transact(func(tx db.Tx) (interface{}, error) {
		return tx.Exec(`INSERT INTO staging_entries (staging_token, path, address, last_modified_date, size, e_tag, metadata)
								VALUES ($1, $2, $3, $4, $5, $6, $7)
								ON CONFLICT (staging_token, path) DO UPDATE
									SET (staging_token, path, address, last_modified_date, size, e_tag, metadata) =
											(excluded.staging_token, excluded.path, excluded.address, excluded.last_modified_date, excluded.size,
											 excluded.e_tag, excluded.metadata)`,
			st, path, entry.Address, entry.LastModified, entry.Size, entry.ETag, entry.Metadata)
	}, p.txOpts(ctx)...)
	return err
}

func (p *PGStagingManager) DeleteEntry(ctx context.Context, st StagingToken, path Path) error {
	_, err := p.db.Transact(func(tx db.Tx) (interface{}, error) {
		return tx.Exec("DELETE FROM staging_entries WHERE staging_token=$1 AND path=$2", st, path)
	}, p.txOpts(ctx)...)
	return err
}

func (p *PGStagingManager) ListEntries(ctx context.Context, st StagingToken) (EntryIterator, error) {
	return NewStagingIterator(ctx, p.db, p.log, st), nil
}

func (p *PGStagingManager) Drop(ctx context.Context, st StagingToken) error {
	_, err := p.db.Transact(func(tx db.Tx) (interface{}, error) {
		return tx.Exec("DELETE FROM staging_entries WHERE staging_token=$1", st)
	}, p.txOpts(ctx)...)
	return err
}

func (p *PGStagingManager) txOpts(ctx context.Context, opts ...db.TxOpt) []db.TxOpt {
	o := []db.TxOpt{
		db.WithContext(ctx),
		db.WithLogger(p.log),
	}
	return append(o, opts...)
}
