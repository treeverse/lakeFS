package graveler

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

	// initPhase turns true when the iterator was created or `SeekGE()` was called.
	// initPhase turns false when Next() is called.
	// When initPhase is true, Value() should return nil.
	initPhase   bool
	idxInBuffer int
	err         error
	dbHasNext   bool
	buffer      []*ValueRecord
	nextFrom    Key
}

func NewStagingIterator(ctx context.Context, db db.Database, log logging.Logger, st StagingToken) *StagingIterator {
	return &StagingIterator{ctx: ctx, st: st, dbHasNext: true, initPhase: true, db: db, log: log, nextFrom: make([]byte, 0)}
}

func (s *StagingIterator) Next() bool {
	if s.err != nil {
		return false
	}
	s.initPhase = false
	s.idxInBuffer++
	if s.idxInBuffer < len(s.buffer) {
		return true
	}
	if !s.dbHasNext {
		return false
	}
	return s.loadBuffer()
}

func (s *StagingIterator) SeekGE(key Key) {
	s.buffer = nil
	s.err = nil
	s.idxInBuffer = 0
	s.nextFrom = key
	s.dbHasNext = true
	s.initPhase = true
}

func (s *StagingIterator) Value() *ValueRecord {
	if s.err != nil || s.idxInBuffer >= len(s.buffer) {
		return nil
	}
	if s.initPhase {
		return nil
	}
	value := s.buffer[s.idxInBuffer]
	if value.Value != nil && value.Identity == nil {
		value.Value = nil
	}
	return value
}

func (s *StagingIterator) Err() error {
	return s.err
}

func (s *StagingIterator) Close() {
}

func (s *StagingIterator) loadBuffer() bool {
	queryResult, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		var res []*ValueRecord
		err := tx.Select(&res, "SELECT key, identity, data "+
			"FROM staging_kv WHERE staging_token=$1 AND key >= $2 ORDER BY key LIMIT $3", s.st, s.nextFrom, batchSize+1)
		return res, err
	}, db.WithLogger(s.log), db.WithContext(s.ctx), db.ReadOnly())
	if err != nil {
		s.err = err
		return false
	}
	values := queryResult.([]*ValueRecord)
	s.idxInBuffer = 0
	if len(values) == batchSize+1 {
		s.nextFrom = values[len(values)-1].Key
		s.buffer = values[:len(values)-1]
		return true
	}
	s.dbHasNext = false
	s.buffer = values
	return len(values) > 0
}
