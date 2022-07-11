package staging

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

type Iterator struct {
	ctx   context.Context
	store kv.StoreMessage
	itr   *kv.PartitionIterator
	st    graveler.StagingToken
	entry *graveler.ValueRecord
	err   error
}

// NewStagingIterator initiates the staging iterator with a batchSize
func NewStagingIterator(ctx context.Context, store kv.StoreMessage, st graveler.StagingToken) (*Iterator, error) {
	itr, err := kv.NewPartitionIterator(ctx, store.Store, (&graveler.StagedEntry{}).ProtoReflect().Type(), string(st))
	if err != nil {
		return nil, err
	}
	return &Iterator{
		ctx:   ctx,
		store: store,
		itr:   itr,
		st:    st,
	}, nil
}

func (s *Iterator) Next() bool {
	if s.Err() != nil {
		return false
	}
	if !s.itr.Next() {
		s.entry = nil
		return false
	}
	entry := s.itr.Entry()
	if entry == nil {
		s.err = graveler.ErrInvalid
		return false
	}
	key := entry.Value.(*graveler.StagedEntry).Key
	value := valueFromProto(entry.Value.(*graveler.StagedEntry))
	s.entry = &graveler.ValueRecord{
		Key:   key,
		Value: value,
	}
	return true
}

func (s *Iterator) SeekGE(key graveler.Key) {
	s.itr.SeekGE(key)
}

func (s *Iterator) Value() *graveler.ValueRecord {
	if s.Err() != nil {
		return nil
	}
	// Tombstone handling
	if s.entry != nil && s.entry.Value != nil && s.entry.Identity == nil {
		s.entry.Value = nil
	}
	return s.entry
}

func (s *Iterator) Err() error {
	if s.err == nil {
		return s.itr.Err()
	}
	return s.err
}

func (s *Iterator) Close() {
	s.itr.Close()
}
