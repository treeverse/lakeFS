package staging

import (
	"context"

	"github.com/treeverse/lakefs/logging"

	"github.com/treeverse/lakefs/catalog/rocks"

	"github.com/treeverse/lakefs/db"
)

type PostgresStagingManager struct {
	db  db.Database
	log logging.Logger
}

func NewPostgresStagingManager(db db.Database) *PostgresStagingManager {
	return &PostgresStagingManager{
		db:  db,
		log: logging.Default().WithField("service_name", "postgres_staging_manager"),
	}
}

func (p *PostgresStagingManager) GetEntry(ctx context.Context, st rocks.StagingToken, path rocks.Path) (*rocks.Entry, error) {
	res, err := p.db.Transact(func(tx db.Tx) (interface{}, error) {
		entry := &rocks.Entry{}
		err := tx.Get(entry, "SELECT address, last_modified_date, size, checksum, metadata FROM staging_entries WHERE staging_token=$1 AND path=$2", st, path)
		return entry, err
	}, p.txOpts(ctx)...)
	if err != nil {
		return nil, err
	}
	entry := res.(*rocks.Entry)
	if entry.Address == "" {
		return nil, nil
	}
	return entry, nil
}

func (p *PostgresStagingManager) SetEntry(ctx context.Context, st rocks.StagingToken, path rocks.Path, entry *rocks.Entry) error {
	if entry == nil {
		entry = &rocks.Entry{}
	}
	_, err := p.db.Transact(func(tx db.Tx) (interface{}, error) {
		return tx.Exec("INSERT INTO staging_entries"+
			"(staging_token, path, address, last_modified_date, size, checksum, metadata) "+
			"VALUES ($1, $2,$3, $4, $5, $6, $7)"+
			"ON CONFLICT (staging_token, path)"+
			"DO UPDATE SET (staging_token, path, addre"+
			"ss, last_modified_date, size, checksum, metadata) ="+
			"(excluded.staging_token, excluded.path, excluded.address, excluded.last_modified_date, excluded.size, excluded.checksum, excluded.metadata)",
			st, path, entry.Address, entry.LastModified, entry.Size, entry.ETag, entry.Metadata)
	}, p.txOpts(ctx)...)
	return err
}

func (p *PostgresStagingManager) DeleteEntry(ctx context.Context, st rocks.StagingToken, path rocks.Path) error {
	_, err := p.db.Transact(func(tx db.Tx) (interface{}, error) {
		return tx.Exec("DELETE FROM staging_entries WHERE staging_token=$1 AND path=$2", st, path)
	}, p.txOpts(ctx)...)
	return err
}

func (p *PostgresStagingManager) ListEntries(ctx context.Context, st rocks.StagingToken, from rocks.Path) (rocks.EntryIterator, error) {
	return NewSnapshotIterator(from, p, ctx, st), nil
}

type listEntriesResult struct {
	entries  []*rocks.EntryRecord
	hasNext  bool
	nextPath rocks.Path
}

func (p *PostgresStagingManager) listEntries(ctx context.Context, st rocks.StagingToken, from rocks.Path, limit int) (*listEntriesResult, error) {
	queryResult, err := p.db.Transact(func(tx db.Tx) (interface{}, error) {
		var res []*rocks.EntryRecord
		err := tx.Select(&res, "SELECT path, address, last_modified_date, size, checksum, metadata "+
			"FROM staging_entries WHERE staging_token=$1 AND path >= $2 ORDER BY path LIMIT $3", st, from, limit+1)
		return res, err
	}, p.txOpts(ctx)...)
	if err != nil {
		return nil, err
	}
	entries := queryResult.([]*rocks.EntryRecord)
	hasNext := false
	var nextPath rocks.Path
	if len(entries) == limit+1 {
		hasNext = true
		nextPath = entries[len(entries)-1].Path
		entries = entries[:len(entries)-1]
	}
	return &listEntriesResult{
		entries:  entries,
		hasNext:  hasNext,
		nextPath: nextPath,
	}, nil
}

func (p *PostgresStagingManager) Drop(ctx context.Context, st rocks.StagingToken) error {
	_, err := p.db.Transact(func(tx db.Tx) (interface{}, error) {
		return tx.Exec("DELETE FROM staging_entries WHERE staging_token=$1", st)
	}, p.txOpts(ctx)...)
	return err
}

func (p *PostgresStagingManager) txOpts(ctx context.Context, opts ...db.TxOpt) []db.TxOpt {
	o := []db.TxOpt{
		db.WithContext(ctx),
		db.WithLogger(p.log),
	}
	return append(o, opts...)
}
