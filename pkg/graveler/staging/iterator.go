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
	entry *graveler.ValueRecord
	err   error
}

// NewStagingIterator initiates the staging iterator with a batchSize
func NewStagingIterator(ctx context.Context, store kv.StoreMessage, st graveler.StagingToken, batchSize int) (*Iterator, error) {
	itr := kv.NewPartitionIterator(ctx, store.Store, (&graveler.StagedEntryData{}).ProtoReflect().Type(), graveler.StagingTokenPartition(st), batchSize)
	return &Iterator{
		ctx:   ctx,
		store: store,
		itr:   itr,
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
	key := entry.Value.(*graveler.StagedEntryData).Key
	value := graveler.StagedEntryFromProto(entry.Value.(*graveler.StagedEntryData))
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
