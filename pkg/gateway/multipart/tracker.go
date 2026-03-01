package multipart

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/kv"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	multipartPrefix = "multipart"
	// compositeKeySeparator is used to separate path and uploadID in the composite key.
	// Using null byte as it's not valid in S3 object keys or upload IDs.
	compositeKeySeparator = "\x00"
)

type Metadata map[string]string

type Upload struct {
	// UploadID A unique identifier for the uploaded part
	UploadID string `db:"upload_id"`
	// Path Multipart path in repository
	Path string `db:"path"`
	// CreationDate Creation date of the part
	CreationDate time.Time `db:"creation_date"`
	// PhysicalAddress Physical address of the part in the storage
	PhysicalAddress string `db:"physical_address"`
	// Metadata Additional metadata as required (by storage vendor etc.)
	Metadata Metadata `db:"metadata"`
	// ContentType Original file's content-type
	ContentType string `db:"content_type"`
}

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

type Tracker interface {
	Create(ctx context.Context, partition string, multipart Upload) error
	Get(ctx context.Context, partition, path, uploadID string) (*Upload, error)
	Delete(ctx context.Context, partition, path, uploadID string) error
	// List returns an iterator over all active multipart uploads in a repository
	List(ctx context.Context, partition string) (UploadIterator, error)
}

type tracker struct {
	store kv.Store
}

var (
	ErrMultipartUploadNotFound = errors.New("multipart upload not found")
	ErrInvalidUploadID         = errors.New("invalid upload id")
)

func NewTracker(store kv.Store) Tracker {
	return &tracker{
		store: store,
	}
}

func multipartFromProto(pb *UploadData) *Upload {
	return &Upload{
		UploadID:        pb.UploadId,
		Path:            pb.Path,
		CreationDate:    pb.CreationDate.AsTime(),
		PhysicalAddress: pb.PhysicalAddress,
		Metadata:        pb.Metadata,
		ContentType:     pb.ContentType,
	}
}

func protoFromMultipart(m *Upload) *UploadData {
	return &UploadData{
		UploadId:        m.UploadID,
		Path:            m.Path,
		CreationDate:    timestamppb.New(m.CreationDate),
		PhysicalAddress: m.PhysicalAddress,
		Metadata:        m.Metadata,
		ContentType:     m.ContentType,
	}
}

// multipartUploadPath creates a key for multipart upload in KV storage.
// Uses kv.FormatPath to create: "multipart/" + path + compositeKeySeparator + uploadID
// This ensures natural lexicographic ordering by path first, then uploadID.
func multipartUploadPath(parts ...string) string {
	return kv.FormatPath(multipartPrefix, strings.Join(parts, compositeKeySeparator))
}

func (m *tracker) Create(ctx context.Context, partition string, multipart Upload) error {
	if multipart.UploadID == "" {
		return ErrInvalidUploadID
	}
	key := []byte(multipartUploadPath(multipart.Path, multipart.UploadID))
	return kv.SetMsgIf(ctx, m.store, partition, key, protoFromMultipart(&multipart), nil)
}

func (m *tracker) Get(ctx context.Context, partition, path, uploadID string) (*Upload, error) {
	if uploadID == "" {
		return nil, ErrInvalidUploadID
	}
	data := &UploadData{}
	key := []byte(multipartUploadPath(path, uploadID))
	_, err := kv.GetMsg(ctx, m.store, partition, key, data)
	if err != nil {
		return nil, err
	}
	return multipartFromProto(data), nil
}

func (m *tracker) Delete(ctx context.Context, partition, path, uploadID string) error {
	if uploadID == "" {
		return ErrInvalidUploadID
	}
	key := []byte(multipartUploadPath(path, uploadID))
	if _, err := m.store.Get(ctx, []byte(partition), key); err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return fmt.Errorf("%w uploadID=%s", ErrMultipartUploadNotFound, uploadID)
		}
		return err
	}

	return m.store.Delete(ctx, []byte(partition), key)
}

func (m *tracker) List(ctx context.Context, partition string) (UploadIterator, error) {
	return newUploadIterator(ctx, m.store, partition)
}
