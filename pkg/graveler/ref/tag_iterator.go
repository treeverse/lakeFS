package ref

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

type TagIterator struct {
	ctx           context.Context
	it            kv.MessageIterator
	err           error
	value         *graveler.TagRecord
	repoPartition string
	store         kv.Store
	closed        bool
}

func NewTagIterator(ctx context.Context, store kv.Store, repo *graveler.RepositoryRecord) (*TagIterator, error) {
	repoPartition := graveler.RepoPartition(repo)
	it, err := kv.NewPrimaryIterator(ctx, store, (&graveler.TagData{}).ProtoReflect().Type(),
		graveler.RepoPartition(repo),
		[]byte(graveler.TagPath("")), kv.IteratorOptionsFrom([]byte("")))
	if err != nil {
		return nil, err
	}
	return &TagIterator{
		ctx:           ctx,
		it:            it,
		store:         store,
		repoPartition: repoPartition,
		closed:        false,
	}, nil
}

func (i *TagIterator) Next() bool {
	if i.Err() != nil || i.closed {
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
	tag, ok := e.Value.(*graveler.TagData)
	if !ok {
		i.err = graveler.ErrReadingFromStore
		return false
	}
	i.value = graveler.TagFromProto(tag)
	return true
}

func (i *TagIterator) SeekGE(id graveler.TagID) {
	if i.Err() != nil {
		return
	}
	i.Close()
	it, err := kv.NewPrimaryIterator(i.ctx, i.store, (&graveler.TagData{}).ProtoReflect().Type(),
		i.repoPartition,
		[]byte(graveler.TagPath("")), kv.IteratorOptionsFrom([]byte(graveler.TagPath(id))))
	i.it = it
	i.err = err
	i.value = nil
	i.closed = err != nil
}

func (i *TagIterator) Value() *graveler.TagRecord {
	if i.Err() != nil {
		return nil
	}
	return i.value
}

func (i *TagIterator) Err() error {
	if i.err != nil {
		return i.err
	}
	if !i.closed {
		return i.it.Err()
	}
	return nil
}

func (i *TagIterator) Close() {
	if i.closed {
		return
	}
	i.it.Close()
	i.closed = true
}
