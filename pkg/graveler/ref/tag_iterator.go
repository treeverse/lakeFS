package ref

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

type KVTagIterator struct {
	it     kv.MessageIterator
	err    error
	value  *graveler.TagRecord
	repoID graveler.RepositoryID
	store  kv.Store
	ctx    context.Context
}

func NewKVTagIterator(ctx context.Context, store *kv.StoreMessage, repositoryID graveler.RepositoryID) (*KVTagIterator, error) {
	it, err := kv.NewPrimaryIterator(ctx, store.Store, (&graveler.TagData{}).ProtoReflect().Type(),
		graveler.TagPartition(repositoryID),
		graveler.TagPath(""), "", true)
	if err != nil {
		return nil, err
	}
	return &KVTagIterator{
		it:     it,
		store:  store.Store,
		repoID: repositoryID,
		ctx:    ctx,
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
		i.err = graveler.ErrNilValue
		return false
	}
	tag, ok := e.Value.(*graveler.TagData)
	if !ok {
		i.err = graveler.ErrCast
		return false
	}
	i.value = &graveler.TagRecord{
		TagID:    graveler.TagID(tag.Id),
		CommitID: graveler.CommitID(tag.CommitId),
	}
	return true
}

func (i *KVTagIterator) SeekGE(id graveler.TagID) {
	it, err := kv.NewPrimaryIterator(i.ctx, i.store, (&graveler.TagData{}).ProtoReflect().Type(),
		graveler.TagPartition(i.repoID),
		graveler.TagPath(""), graveler.TagPath(id), false)
	i.it = it
	i.value = nil
	i.err = err
}

func (i *KVTagIterator) Value() *graveler.TagRecord {
	if i.Err() != nil {
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
