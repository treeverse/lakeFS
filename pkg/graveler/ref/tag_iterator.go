package ref

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

type KVTagIterator struct {
	it    kv.MessageIterator
	err   error
	value *graveler.TagRecord
	store kv.Store
}

func NewKVTagIterator(ctx context.Context, store kv.StoreMessage, repositoryID graveler.RepositoryID) (*KVTagIterator, error) {
	it, err := kv.NewPrimaryIterator(ctx, store.Store, (&graveler.TagData{}).ProtoReflect().Type(),
		graveler.TagPartition(repositoryID.String()),
		graveler.TagPath(""), "")
	if err != nil {
		return nil, err
	}
	return &KVTagIterator{
		it:    it,
		store: store.Store,
	}, nil
}

func (i *KVTagIterator) Next() bool {
	if i.Err() != nil {
		return false
	}
	if !i.it.Next() {
		i.value = nil
		return false
	}
	e := i.it.Entry()
	if e == nil {
		i.err = graveler.ErrNotFound
		return false
	}
	tag := e.Value.(*graveler.TagData)
	i.value = &graveler.TagRecord{
		TagID:    graveler.TagID(tag.Id),
		CommitID: graveler.CommitID(tag.CommitId),
	}
	return true
}

func (i *KVTagIterator) SeekGE(ctx context.Context, id graveler.TagID, repoId graveler.RepositoryID) error {
	it, err := kv.NewPrimaryIterator(ctx, i.store, (&graveler.TagData{}).ProtoReflect().Type(),
		graveler.TagPartition(repoId.String()),
		graveler.TagPath(""), id.String())
	i.it = it
	return err
}

func (i *KVTagIterator) Value() *graveler.TagRecord {
	if i.err != nil {
		return nil
	}
	return i.value
}

func (i *KVTagIterator) Err() error {
	if i.err == nil {
		return i.it.Err()
	}
	return i.err
}

func (i *KVTagIterator) Close() {
	i.it.Close()
}
