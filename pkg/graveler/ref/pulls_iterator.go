package ref

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

type PullsIterator struct {
	ctx           context.Context
	it            kv.MessageIterator
	err           error
	value         *graveler.PullRequestRecord
	repoPartition string
	store         kv.Store
	closed        bool
}

func NewPullsIterator(ctx context.Context, store kv.Store, repo *graveler.RepositoryRecord) (*PullsIterator, error) {
	repoPartition := graveler.RepoPartition(repo)
	it, err := kv.NewPrimaryIterator(ctx, store, (&graveler.PullRequestData{}).ProtoReflect().Type(),
		graveler.RepoPartition(repo),
		[]byte(PullRequestPath("")), kv.IteratorOptionsFrom([]byte("")))
	if err != nil {
		return nil, err
	}
	return &PullsIterator{
		ctx:           ctx,
		it:            it,
		store:         store,
		repoPartition: repoPartition,
		closed:        false,
	}, nil
}

func (i *PullsIterator) Next() bool {
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
	pull, ok := e.Value.(*graveler.PullRequestData)
	if !ok {
		i.err = graveler.ErrReadingFromStore
		return false
	}
	i.value = graveler.PullRequestFromProto(pull)
	return true
}

func (i *PullsIterator) SeekGE(id graveler.PullRequestID) {
	if i.Err() != nil {
		return
	}
	i.Close()
	it, err := kv.NewPrimaryIterator(i.ctx, i.store, (&graveler.PullRequestData{}).ProtoReflect().Type(),
		i.repoPartition,
		[]byte(PullRequestPath("")), kv.IteratorOptionsFrom([]byte(PullRequestPath(id))))
	i.it = it
	i.err = err
	i.value = nil
	i.closed = err != nil
}

func (i *PullsIterator) Value() *graveler.PullRequestRecord {
	if i.Err() != nil {
		return nil
	}
	return i.value
}

func (i *PullsIterator) Err() error {
	if i.err != nil {
		return i.err
	}
	if !i.closed {
		return i.it.Err()
	}
	return nil
}

func (i *PullsIterator) Close() {
	if i.closed {
		return
	}
	i.it.Close()
	i.closed = true
}
