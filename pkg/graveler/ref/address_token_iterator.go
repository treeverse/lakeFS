package ref

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

// AddressTokenIterator Iterates over repository's token addresses
type AddressTokenIterator struct {
	ctx           context.Context
	it            kv.MessageIterator
	err           error
	value         *graveler.LinkAddressData
	repoPartition string
	store         kv.Store
	closed        bool
}

func NewAddressTokenIterator(ctx context.Context, store *kv.StoreMessage, repo *graveler.RepositoryRecord) (*AddressTokenIterator, error) {
	repoPartition := graveler.RepoPartition(repo)
	it, err := kv.NewPrimaryIterator(ctx, store.Store, (&graveler.LinkAddressData{}).ProtoReflect().Type(),
		repoPartition, []byte(graveler.LinkedAddressPath("")), kv.IteratorOptionsFrom([]byte("")))
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
	entry := i.it.Entry()
	if entry == nil {
		i.err = graveler.ErrReadingFromStore
		return false
	}
	token, ok := entry.Value.(*graveler.LinkAddressData)
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
	i.it, i.err = kv.NewPrimaryIterator(i.ctx, i.store, (&graveler.LinkAddressData{}).ProtoReflect().Type(),
		i.repoPartition,
		[]byte(graveler.LinkedAddressPath("")), kv.IteratorOptionsFrom([]byte(graveler.LinkedAddressPath(address))))
	i.value = nil
	i.closed = i.err != nil
}

func (i *AddressTokenIterator) Value() *graveler.LinkAddressData {
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
