package multipart

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

// kvUploadIterator streams uploads directly from KV store using natural ordering of composite keys
type kvUploadIterator struct {
	kvIter        *kv.PrimaryIterator
	store         kv.Store
	ctx           context.Context
	repoPartition string
	err           error
	value         *Upload
}

// newUploadIterator creates an iterator that streams from KV store
// Uses composite key (path + uploadID) for natural ordering - no in-memory sorting needed
func newUploadIterator(ctx context.Context, store kv.Store, partitionKey string) (*kvUploadIterator, error) {
	kvIter, err := kv.NewPrimaryIterator(ctx, store, (&UploadData{}).ProtoReflect().Type(), partitionKey, []byte(multipartUploadPath()), kv.IteratorOptionsAfter([]byte("")))
	if err != nil {
		return nil, err
	}
	return &kvUploadIterator{
		kvIter:        kvIter,
		store:         store,
		ctx:           ctx,
		repoPartition: partitionKey,
	}, nil
}

func (it *kvUploadIterator) Next() bool {
	if it.Err() != nil {
		return false
	}
	if !it.kvIter.Next() {
		it.value = nil
		return false
	}
	entry := it.kvIter.Entry()
	if entry == nil {
		it.err = graveler.ErrReadingFromStore
		return false
	}
	value, ok := entry.Value.(*UploadData)
	if !ok {
		it.err = graveler.ErrReadingFromStore
		return false
	}
	it.value = multipartFromProto(value)
	return true
}

func (it *kvUploadIterator) Value() *Upload {
	if it.Err() != nil {
		return nil
	}
	return it.value
}

func (it *kvUploadIterator) Err() error {
	if it.err != nil {
		return it.err
	}
	return it.kvIter.Err()
}

func (it *kvUploadIterator) Close() {
	it.kvIter.Close()
}

func (it *kvUploadIterator) SeekGE(key, uploadID string) {
	it.Close()
	itr, err := kv.NewPrimaryIterator(it.ctx, it.store, (&UploadData{}).ProtoReflect().Type(),
		it.repoPartition,
		[]byte(multipartUploadPath()), kv.IteratorOptionsFrom([]byte(multipartUploadPath(key, uploadID))))
	if err != nil {
		it.err = err
	} else { // assign only when no error occurred
		it.kvIter = itr
	}
	it.value = nil
}
