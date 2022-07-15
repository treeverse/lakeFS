package ref

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

type KVOrderedCommitIterator struct {
	it     kv.MessageIterator
	err    error
	value  *graveler.CommitRecord
	repoID graveler.RepositoryID
	store  kv.Store
	ctx    context.Context
}

func NewKVOrderedCommitIterator(ctx context.Context, store *kv.StoreMessage, repositoryID graveler.RepositoryID) (*KVOrderedCommitIterator, error) {
	it, err := kv.NewPrimaryIterator(ctx, store.Store, (&graveler.TagData{}).ProtoReflect().Type(),
		graveler.CommitPartition(repositoryID),
		[]byte(graveler.CommitPath("")), []byte(""), true)
	if err != nil {
		return nil, err
	}
	return &KVOrderedCommitIterator{
		it:     it,
		store:  store.Store,
		repoID: repositoryID,
		ctx:    ctx,
	}, nil
}

func (i *KVOrderedCommitIterator) Next() bool {
	if i.Err() != nil {
		return false
	}
	if !i.it.Next() {
		i.value = nil
		return false
	}
	e := i.it.Entry()
	if e == nil {
		i.err = graveler.ErrReadingFromStore
		return false
	}
	c, ok := e.Value.(*graveler.CommitData)
	if !ok {
		i.err = graveler.ErrReadingFromStore
		return false
	}
	i.value = CommitDataToCommitRecord(c)
	return true
}

func (i *KVOrderedCommitIterator) SeekGE(id graveler.CommitID) {
	if i.Err() == nil {
		i.it.Close()
		it, err := kv.NewPrimaryIterator(i.ctx, i.store, (&graveler.CommitData{}).ProtoReflect().Type(),
			graveler.CommitPartition(i.repoID),
			[]byte(graveler.CommitPath("")), []byte(graveler.CommitPath(id)), false)
		i.it = it
		i.value = nil
		i.err = err
	}
}

func (i *KVOrderedCommitIterator) Value() *graveler.CommitRecord {
	if i.Err() != nil {
		return nil
	}
	return i.value
}

func (i *KVOrderedCommitIterator) Err() error {
	if i.err == nil {
		return i.it.Err()
	}
	return i.err
}

func (i *KVOrderedCommitIterator) Close() {
	i.it.Close()
}
