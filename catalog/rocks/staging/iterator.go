package staging

import (
	"context"

	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

const batchSize = 1000

type Iterator struct {
	ctx context.Context
	db  db.Database
	log logging.Logger
	st  rocks.StagingToken

	idxInBuffer int
	err         error
	dbHasNext   bool
	buffer      []*rocks.EntryRecord
	nextFrom    rocks.Path
}

func NewIterator(ctx context.Context, db db.Database, log logging.Logger, st rocks.StagingToken) *Iterator {
	return &Iterator{ctx: ctx, st: st, dbHasNext: true, db: db, log: log}
}

func (s *Iterator) Next() bool {
	if s.err != nil {
		return false
	}
	s.idxInBuffer++
	if s.idxInBuffer < len(s.buffer) {
		return true
	}
	if !s.dbHasNext {
		return false
	}
	res, err := s.listEntries(s.ctx, s.st, s.nextFrom, batchSize)
	if err != nil {
		s.err = err
		return false
	}
	if len(res.entries) == 0 {
		s.dbHasNext = false
		return false
	}
	if res.hasNext {
		s.nextFrom = res.nextPath
	}
	s.dbHasNext = res.hasNext
	s.idxInBuffer = 0
	s.buffer = res.entries
	return true
}

func (s *Iterator) SeekGE(path rocks.Path) bool {
	s.buffer = nil
	s.err = nil
	s.idxInBuffer = 0
	s.nextFrom = path
	s.dbHasNext = true
	return s.Next()
}

func (s *Iterator) Value() *rocks.EntryRecord {
	if s.err != nil || s.idxInBuffer >= len(s.buffer) {
		return nil
	}
	return s.buffer[s.idxInBuffer]
}

func (s *Iterator) Err() error {
	return s.err
}

func (s *Iterator) Close() {
}

func (s *Iterator) txOpts(ctx context.Context, opts ...db.TxOpt) []db.TxOpt {
	o := []db.TxOpt{
		db.WithContext(ctx),
		db.WithLogger(s.log),
	}
	return append(o, opts...)
}
func (s *Iterator) listEntries(ctx context.Context, st rocks.StagingToken, from rocks.Path, limit int) (*listEntriesResult, error) {
	queryResult, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		var res []*rocks.EntryRecord
		err := tx.Select(&res, "SELECT path, address, last_modified_date, size, checksum, metadata "+
			"FROM staging_entries WHERE staging_token=$1 AND path >= $2 ORDER BY path LIMIT $3", st, from, limit+1)
		return res, err
	}, s.txOpts(ctx, db.ReadOnly())...)
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
