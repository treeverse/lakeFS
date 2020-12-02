package rocks

import (
	"context"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

const batchSize = 1000

type StagingIterator struct {
	ctx context.Context
	db  db.Database
	log logging.Logger
	st  StagingToken

	idxInBuffer int
	err         error
	dbHasNext   bool
	buffer      []*EntryRecord
	nextFrom    Path
}

func NewStagingIterator(ctx context.Context, db db.Database, log logging.Logger, st StagingToken) *StagingIterator {
	return &StagingIterator{ctx: ctx, st: st, dbHasNext: true, db: db, log: log}
}

func (s *StagingIterator) Next() bool {
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
	return s.loadBuffer()
}

func (s *StagingIterator) SeekGE(path Path) bool {
	s.buffer = nil
	s.err = nil
	s.idxInBuffer = 0
	s.nextFrom = path
	s.dbHasNext = true
	return s.Next()
}

func (s *StagingIterator) Value() *EntryRecord {
	if s.err != nil || s.idxInBuffer >= len(s.buffer) {
		return nil
	}
	return s.buffer[s.idxInBuffer]
}

func (s *StagingIterator) Err() error {
	return s.err
}

func (s *StagingIterator) Close() {
}

func (s *StagingIterator) loadBuffer() bool {
	queryResult, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		var res []*EntryRecord
		err := tx.Select(&res, "SELECT path, address, last_modified_date, size, e_tag, metadata "+
			"FROM staging_entries WHERE staging_token=$1 AND path >= $2 ORDER BY path LIMIT $3", s.st, s.nextFrom, batchSize+1)
		return res, err
	}, db.WithLogger(s.log), db.WithContext(s.ctx), db.ReadOnly())
	if err != nil {
		s.err = err
		return false
	}
	entries := queryResult.([]*EntryRecord)
	s.idxInBuffer = 0
	if len(entries) == batchSize+1 {
		s.nextFrom = entries[len(entries)-1].Path
		s.buffer = entries[:len(entries)-1]
		return true
	}
	s.dbHasNext = false
	s.buffer = entries
	return len(entries) > 0
}
