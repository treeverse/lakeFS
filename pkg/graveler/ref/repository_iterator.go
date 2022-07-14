package ref

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

type RepositoryIterator interface {
	Next() bool
	SeekGE(id graveler.RepositoryID)
	Value() *graveler.RepositoryRecord
	Err() error
	Close()
}

type KVRepositoryIterator struct {
	ctx   context.Context
	it    kv.MessageIterator
	err   error
	value *graveler.RepositoryRecord
	store kv.Store
}

func NewKVRepositoryIterator(ctx context.Context, store *kv.StoreMessage) (*KVRepositoryIterator, error) {
	it, err := kv.NewPrimaryIterator(ctx, store.Store, (&graveler.RepositoryData{}).ProtoReflect().Type(), graveler.RepositoriesPartition(), []byte(graveler.RepoPath("")), kv.IteratorOptionsAfter([]byte{}))
	if err != nil {
		return nil, err
	}
	return &KVRepositoryIterator{
		it:    it,
		store: store.Store,
		ctx:   ctx,
	}, nil
}

func (ri *KVRepositoryIterator) Next() bool {
	if ri.Err() != nil {
		return false
	}

	if !ri.it.Next() {
		ri.value = nil
		return false
	}
	e := ri.it.Entry()
	if e == nil {
		ri.err = graveler.ErrInvalid
		return false
	}

	repo, ok := e.Value.(*graveler.RepositoryData)
	if repo == nil || !ok {
		ri.err = graveler.ErrReadingFromStore
		return false
	}

	ri.value = graveler.RepoFromProto(repo)
	return true
}

func (ri *KVRepositoryIterator) SeekGE(id graveler.RepositoryID) {
	if errors.Is(ri.Err(), kv.ErrClosedEntries) {
		return
	}
	ri.it.Close()
	ri.it, ri.err = kv.NewPrimaryIterator(ri.ctx, ri.store, (&graveler.RepositoryData{}).ProtoReflect().Type(), graveler.RepositoriesPartition(), []byte(graveler.RepoPath("")), kv.IteratorOptionsFrom([]byte(graveler.RepoPath(id))))
	ri.value = nil
}

func (ri *KVRepositoryIterator) Value() *graveler.RepositoryRecord {
	if ri.Err() != nil {
		return nil
	}
	return ri.value
}

func (ri *KVRepositoryIterator) Err() error {
	if ri.err == nil {
		return ri.it.Err()
	}
	return ri.err
}

func (ri *KVRepositoryIterator) Close() {
	ri.it.Close()
	ri.err = ErrIteratorClosed
}
