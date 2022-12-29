package ref

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

// AddressTokenIterator Iterates over repository's token addresses
type AddressTokenIterator struct {
	//ctx           context.Context
	//store         *kv.StoreMessage
	//itr           *kv.PrimaryIterator
	//repoPartition string
	//value         *graveler.AddressData
	//err           error
	//
	//
	ctx           context.Context
	it            kv.MessageIterator
	err           error
	value         *graveler.AddressData
	repoPartition string
	store         kv.Store
	closed        bool
}

func NewAddressTokenIterator(ctx context.Context, store *kv.StoreMessage, repo *graveler.RepositoryRecord) (*AddressTokenIterator, error) {
	repoPartition := graveler.RepoPartition(repo)
	it, err := kv.NewPrimaryIterator(ctx, store.Store, (&graveler.AddressData{}).ProtoReflect().Type(),
		repoPartition, []byte(graveler.AddressPath("")), kv.IteratorOptionsFrom([]byte("")))
	if err != nil {
		return nil, err
	}

	return &AddressTokenIterator{
		ctx:           ctx,
		store:         store.Store,
		it:            it,
		repoPartition: repoPartition,
		value:         nil,
		err:           nil,
	}, nil
}

func (i *AddressTokenIterator) Next() bool {
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
	token, ok := e.Value.(*graveler.AddressData)
	if !ok {
		i.err = graveler.ErrReadingFromStore
		return false
	}
	i.value = token
	return true
}

func (i *AddressTokenIterator) SeekGE(address string) {
	if i.Err() != nil {
		return
	}
	i.Close()
	it, err := kv.NewPrimaryIterator(i.ctx, i.store, (&graveler.AddressData{}).ProtoReflect().Type(),
		i.repoPartition,
		[]byte(graveler.TagPath("")), kv.IteratorOptionsFrom([]byte(graveler.AddressPath(address))))
	i.it = it
	i.err = err
	i.value = nil
	i.closed = err != nil
}

func (i *AddressTokenIterator) Value() *graveler.AddressData {
	if i.Err() != nil {
		return nil
	}
	return i.value
}

func (i *AddressTokenIterator) Err() error {
	if i.err != nil {
		return i.err
	}
	if !i.closed {
		return i.it.Err()
	}
	return nil
}

func (i *AddressTokenIterator) Close() {
	if i.closed {
		return
	}
	i.it.Close()
	i.closed = true
}
