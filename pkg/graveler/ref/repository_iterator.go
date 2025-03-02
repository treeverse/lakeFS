package ref

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

func updateStorageID(repo *graveler.RepositoryRecord, storageConfig config.StorageConfig) {
	if repo.StorageID == config.SingleBlockstoreID {
		if storage := storageConfig.GetStorageByID(config.SingleBlockstoreID); storage != nil {
			repo.StorageID = graveler.StorageID(storage.ID()) // Will return the real actual ID
		}
	}
}

type RepositoryIterator struct {
	ctx           context.Context
	storageConfig config.StorageConfig
	it            kv.MessageIterator
	err           error
	value         *graveler.RepositoryRecord
	store         kv.Store
	closed        bool
}

func NewRepositoryIterator(ctx context.Context, store kv.Store, storageConfig config.StorageConfig) (*RepositoryIterator, error) {
	it, err := kv.NewPrimaryIterator(ctx, store, (&graveler.RepositoryData{}).ProtoReflect().Type(), graveler.RepositoriesPartition(), []byte(graveler.RepoPath("")), kv.IteratorOptionsAfter([]byte{}))
	if err != nil {
		return nil, err
	}
	return &RepositoryIterator{
		ctx:           ctx,
		storageConfig: storageConfig,
		it:            it,
		store:         store,
		closed:        false,
	}, nil
}

func (ri *RepositoryIterator) Next() bool {
	if ri.Err() != nil || ri.closed {
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
	ri.value.StorageID = graveler.StorageID(config.GetActualStorageID(ri.storageConfig, ri.value.StorageID.String()))
	updateStorageID(ri.value, ri.storageConfig)
	return true
}

func (ri *RepositoryIterator) SeekGE(id graveler.RepositoryID) {
	if errors.Is(ri.Err(), kv.ErrClosedEntries) {
		return
	}
	ri.Close()
	ri.it, ri.err = kv.NewPrimaryIterator(ri.ctx, ri.store, (&graveler.RepositoryData{}).ProtoReflect().Type(), graveler.RepositoriesPartition(), []byte(graveler.RepoPath("")), kv.IteratorOptionsFrom([]byte(graveler.RepoPath(id))))
	ri.closed = ri.err != nil
	ri.value = nil
}

func (ri *RepositoryIterator) Value() *graveler.RepositoryRecord {
	if ri.Err() != nil {
		return nil
	}
	return ri.value
}

func (ri *RepositoryIterator) Err() error {
	if ri.err != nil {
		return ri.err
	}
	if !ri.closed {
		return ri.it.Err()
	}
	return nil
}

func (ri *RepositoryIterator) Close() {
	if ri.closed {
		return
	}
	ri.it.Close()
	ri.closed = true
}
