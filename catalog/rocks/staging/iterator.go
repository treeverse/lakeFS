package staging

import (
	"context"

	"github.com/treeverse/lakefs/catalog/rocks"
)

const batchSize = 1000

type SnapshotIterator struct {
	buffer      []*rocks.EntryRecord
	nextFrom    rocks.Path
	idxInBuffer int
	err         error
	mgr         *PostgresStagingManager
	ctx         context.Context
	st          rocks.StagingToken
	dbHasNext   bool
}

func NewSnapshotIterator(nextFrom rocks.Path, mgr *PostgresStagingManager, ctx context.Context, st rocks.StagingToken) *SnapshotIterator {
	return &SnapshotIterator{nextFrom: nextFrom, mgr: mgr, ctx: ctx, st: st, dbHasNext: true}
}

func (s *SnapshotIterator) Next() bool {
	s.idxInBuffer++
	if s.idxInBuffer >= len(s.buffer) {
		if !s.dbHasNext {
			return false
		}
		res, err := s.mgr.listEntries(s.ctx, s.st, s.nextFrom, batchSize)
		if err != nil {
			s.err = err
			return false
		}
		if len(res.entries) == 0 {
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
	return true
}

func (s *SnapshotIterator) SeekGE(path rocks.Path) bool {
	s.buffer = nil
	s.idxInBuffer = 0
	s.nextFrom = path
	s.dbHasNext = true
	return s.Next()
}

func (s *SnapshotIterator) Value() *rocks.EntryRecord {
	return s.buffer[s.idxInBuffer]
}

func (s *SnapshotIterator) Err() error {
	return s.err
}

func (s *SnapshotIterator) Close() {
}
