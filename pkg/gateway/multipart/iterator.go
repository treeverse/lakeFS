package multipart

import (
	"context"
	"fmt"
	"sort"

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
	// SeekGE seeks to the first upload with Path > key, or if Path == key, UploadID > uploadID
	// After calling SeekGE, Next() must be called to access the first element at or after the seek position
	SeekGE(key string, uploadID string)
}

// sortedUploadIterator collects all uploads, sorts them, and provides efficient seeking
type sortedUploadIterator struct {
	uploads []*Upload
	current int
	err     error
}

// newSortedUploadIterator creates a sorted iterator from the kv store
// It loads all uploads into memory and sorts them by Path, then UploadID
func newSortedUploadIterator(ctx context.Context, store kv.Store, partitionKey string) (*sortedUploadIterator, error) {
	kvIter := kv.NewPartitionIterator(ctx, store, (&UploadData{}).ProtoReflect().Type(), partitionKey, 0)
	defer kvIter.Close()

	var uploads []*Upload
	for kvIter.Next() {
		entry := kvIter.Entry()
		if entry == nil {
			continue
		}
		data, ok := entry.Value.(*UploadData)
		if !ok {
			panic(fmt.Sprintf("unexpected value type: %T", entry.Value))
		}
		uploads = append(uploads, multipartFromProto(data))
	}
	if err := kvIter.Err(); err != nil {
		return nil, err
	}

	// Sort by Path (ascending), then by UploadID (ascending)
	sort.Slice(uploads, func(i, j int) bool {
		if uploads[i].Path == uploads[j].Path {
			return uploads[i].UploadID < uploads[j].UploadID
		}
		return uploads[i].Path < uploads[j].Path
	})

	return &sortedUploadIterator{
		uploads: uploads,
		current: -1, // Start before first element
	}, nil
}

func (s *sortedUploadIterator) Next() bool {
	if s.err != nil {
		return false
	}
	s.current++
	return s.current < len(s.uploads)
}

func (s *sortedUploadIterator) Value() *Upload {
	if s.current < 0 || s.current >= len(s.uploads) {
		return nil
	}
	return s.uploads[s.current]
}

func (s *sortedUploadIterator) Err() error {
	return s.err
}

func (s *sortedUploadIterator) Close() {
	// No resources to release for in-memory iterator
}

func (s *sortedUploadIterator) SeekGE(key string, uploadID string) {
	// Binary search to find the first upload > (key, uploadID) in lexicographic order
	// Per AWS S3 spec: markers are excluded from results, only items after the marker are returned
	s.current = sort.Search(len(s.uploads), func(i int) bool {
		upload := s.uploads[i]
		if upload.Path > key {
			return true
		}
		if upload.Path == key && upload.UploadID > uploadID {
			return true
		}
		return false
	})
	// Adjust to be one before the target position
	// because Next() will increment current
	s.current--
}
