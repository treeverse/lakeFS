package multiparts

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/pkg/kv"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	multipartsPartitionKey = "multiparts"
)

type Metadata map[string]string

type MultipartUpload struct {
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

type Tracker interface {
	Create(ctx context.Context, multipart MultipartUpload) error
	Get(ctx context.Context, uploadID string) (*MultipartUpload, error)
	Delete(ctx context.Context, uploadID string) error
}

type tracker struct {
	store kv.StoreMessage
}

var (
	ErrMultipartUploadNotFound = errors.New("multipart upload not found")
	ErrInvalidUploadID         = errors.New("invalid upload id")
)

func NewTracker(ms kv.StoreMessage) Tracker {
	return &tracker{
		store: ms,
	}
}

func multipartFromProto(pb *MultipartUploadData) *MultipartUpload {
	return &MultipartUpload{
		UploadID:        pb.UploadId,
		Path:            pb.Path,
		CreationDate:    pb.CreationDate.AsTime(),
		PhysicalAddress: pb.PhysicalAddress,
		Metadata:        pb.Metadata,
		ContentType:     pb.ContentType,
	}
}

func protoFromMultipart(m *MultipartUpload) *MultipartUploadData {
	return &MultipartUploadData{
		UploadId:        m.UploadID,
		Path:            m.Path,
		CreationDate:    timestamppb.New(m.CreationDate),
		PhysicalAddress: m.PhysicalAddress,
		Metadata:        m.Metadata,
		ContentType:     m.ContentType,
	}
}

func (m *tracker) Create(ctx context.Context, multipart MultipartUpload) error {
	if multipart.UploadID == "" {
		return ErrInvalidUploadID
	}
	return m.store.SetMsgIf(ctx, multipartsPartitionKey, []byte(multipart.UploadID), protoFromMultipart(&multipart), nil)
}

func (m *tracker) Get(ctx context.Context, uploadID string) (*MultipartUpload, error) {
	if uploadID == "" {
		return nil, ErrInvalidUploadID
	}
	data := &MultipartUploadData{}
	_, err := m.store.GetMsg(ctx, multipartsPartitionKey, []byte(uploadID), data)
	if err != nil {
		return nil, err
	}
	return multipartFromProto(data), nil
}

func (m *tracker) Delete(ctx context.Context, uploadID string) error {
	if uploadID == "" {
		return ErrInvalidUploadID
	}
	store := m.store.Store
	key := []byte(uploadID)
	if _, err := store.Get(ctx, []byte(multipartsPartitionKey), key); err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return fmt.Errorf("%w uploadID=%s", ErrMultipartUploadNotFound, uploadID)
		}
		return err
	}

	return store.Delete(ctx, []byte(multipartsPartitionKey), key)
}
