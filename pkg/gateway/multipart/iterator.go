package multipart

import (
	"context"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
)

// UploadIterator is an iterator over multipart uploads sorted by Path, then UploadID
type UploadIterator interface {
	// Next advances the iterator to the next upload
	// Returns true if there is a next upload, false otherwise
	Next() bool
	// Value returns the current upload
	// Should only be called after Next returns true
	Value() *Upload
	// Err returns any error encountered during iteration
	Err() error
	// Close releases resources associated with the iterator
	Close()
	// SeekGE seeks to the first upload with key >= uploadIDKey(path, uploadID)
	// After calling SeekGE, Next() must be called to access the first element at or after the seek position
	SeekGE(key, uploadID string)
}

// kvUploadIterator streams uploads directly from KV store using natural ordering of composite keys
type kvUploadIterator struct {
	kvIter        *kv.PrimaryIterator
	store         kv.Store
	ctx           context.Context
	repoPartition string
	err           error
	value         *Upload
	closed        bool
}

// newUploadIterator creates an iterator that streams from KV store
// Uses composite key (path + uploadID) for natural ordering - no in-memory sorting needed
func newUploadIterator(ctx context.Context, store kv.Store, partitionKey string) (UploadIterator, error) {
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
	if it.Err() != nil || it.closed {
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
	if !it.closed {
		return it.kvIter.Err()
	}
	return nil
}

func (it *kvUploadIterator) Close() {
	if it.closed {
		return
	}
	it.kvIter.Close()
	it.closed = true
}

func (it *kvUploadIterator) SeekGE(key, uploadID string) {
	if it.Err() != nil {
		return
	}
	it.Close()
	itr, err := kv.NewPrimaryIterator(it.ctx, it.store, (&UploadData{}).ProtoReflect().Type(),
		it.repoPartition,
		[]byte(multipartUploadPath()), kv.IteratorOptionsFrom([]byte(multipartUploadPath(key, uploadID))))
	it.kvIter = itr
	it.err = err
	it.value = nil
	it.closed = err != nil
}
