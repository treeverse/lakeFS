package staging

import (
	"context"

	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
)

type DBIterator struct {
	ctx context.Context
	db  db.Database
	log logging.Logger
	st  graveler.StagingToken

	// initPhase turns true when the iterator was created or `SeekGE()` was called.
	// initPhase turns false when Next() is called.
	// When initPhase is true, Value() should return nil.
	initPhase   bool
	idxInBuffer int
	err         error
	dbHasNext   bool
	buffer      []*graveler.ValueRecord
	nextFrom    graveler.Key
	batchSize   int
}

// NewDBStagingIterator initiates the staging iterator with a batchSize
func NewDBStagingIterator(ctx context.Context, db db.Database, log logging.Logger, st graveler.StagingToken, batchSize int) *DBIterator {
	bs := batchSize
	if bs <= 0 {
		bs = graveler.ListingDefaultBatchSize
	} else if bs > graveler.ListingMaxBatchSize {
		bs = graveler.ListingMaxBatchSize
	}
	return &DBIterator{ctx: ctx, st: st, dbHasNext: true, initPhase: true, db: db, log: log, nextFrom: make([]byte, 0), batchSize: bs}
}

func (s *DBIterator) Next() bool {
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

func (s *DBIterator) SeekGE(key graveler.Key) {
	s.buffer = nil
	s.err = nil
	s.idxInBuffer = 0
	s.nextFrom = key
	s.dbHasNext = true
	s.initPhase = true
}

func (s *DBIterator) Value() *graveler.ValueRecord {
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

func (s *DBIterator) Err() error {
	return s.err
}

func (s *DBIterator) Close() {
}

func (s *DBIterator) loadBuffer() bool {
	queryResult, err := s.db.Transact(s.ctx, func(tx db.Tx) (interface{}, error) {
		var res []*graveler.ValueRecord
		err := tx.Select(&res, "SELECT key, identity, data "+
			"FROM graveler_staging_kv WHERE staging_token=$1 AND key >= $2 ORDER BY key LIMIT $3", s.st, s.nextFrom, s.batchSize+1)
		return res, err
	}, db.WithLogger(s.log), db.ReadOnly())
	if err != nil {
		s.err = err
		return false
	}
	values := queryResult.([]*graveler.ValueRecord)
	s.idxInBuffer = 0
	if len(values) == s.batchSize+1 {
		s.nextFrom = values[len(values)-1].Key
		s.buffer = values[:len(values)-1]
		return true
	}
	s.dbHasNext = false
	s.buffer = values
	return len(values) > 0
}
