package staging

import (
	"context"

	"github.com/treeverse/lakefs/catalog/rocks"
)

const batchSize = 1000

type Iterator struct {
	mgr         *Manager
	nextFrom    rocks.Path
	idxInBuffer int
	err         error
	ctx         context.Context
	st          rocks.StagingToken
	dbHasNext   bool
	buffer      []*rocks.EntryRecord
}

func NewIterator(ctx context.Context, mgr *Manager, st rocks.StagingToken, nextFrom rocks.Path) *Iterator {
	return &Iterator{nextFrom: nextFrom, mgr: mgr, ctx: ctx, st: st, dbHasNext: true}
}

func (s *Iterator) Next() bool {
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

func (s *Iterator) SeekGE(path rocks.Path) bool {
	s.buffer = nil
	s.idxInBuffer = 0
	s.nextFrom = path
	s.dbHasNext = true
	return s.Next()
}

func (s *Iterator) Value() *rocks.EntryRecord {
	return s.buffer[s.idxInBuffer]
}

func (s *Iterator) Err() error {
	return s.err
}

func (s *Iterator) Close() {
}
