package multiparts

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	kv "github.com/treeverse/lakefs/pkg/kv"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Metadata map[string]string

var ErrAlreadyExists = errors.New("upload ID already exists")

type MultipartUpload struct {
	UploadID        string    `db:"upload_id"`
	Path            string    `db:"path"`
	CreationDate    time.Time `db:"creation_date"`
	PhysicalAddress string    `db:"physical_address"`
	Metadata        Metadata  `db:"metadata"`
	ContentType     string    `db:"content_type"`
}

func (m Metadata) Set(k, v string) {
	m[strings.ToLower(k)] = v
}

func (m Metadata) Get(k string) string {
	return m[strings.ToLower(k)]
}
func (m Metadata) Value() (driver.Value, error) {
	return json.Marshal(m)
}

func (m *Metadata) Scan(src interface{}) error {
	if src == nil {
		return nil
	}
	data, ok := src.([]byte)
	if !ok {
		return ErrInvalidMetadataSrcFormat
	}
	return json.Unmarshal(data, m)
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
	ErrMultipartUploadNotFound  = errors.New("multipart upload not found")
	ErrInvalidUploadID          = errors.New("invalid upload id")
	ErrInvalidMetadataSrcFormat = errors.New("invalid metadata source format")
)

func NewTracker(ms kv.StoreMessage) Tracker {
	return &tracker{
		store: ms,
	}
}

func (m *tracker) Create(ctx context.Context, multipart MultipartUpload) error {
	if multipart.UploadID == "" {
		return ErrInvalidUploadID
	}
	_, err := m.Get(ctx, multipart.UploadID)
	if err == nil {
		return ErrAlreadyExists
	}

	err = m.store.SetMsg(ctx, []byte(multipart.UploadID), &MultipartUploadData{
		UploadId:        multipart.UploadID,
		Path:            multipart.Path,
		CreationDate:    timestamppb.New(multipart.CreationDate),
		PhysicalAddress: multipart.PhysicalAddress,
		Metadata:        multipart.Metadata,
		ContentType:     multipart.ContentType,
	})
	return err
}

func (m *tracker) Get(ctx context.Context, uploadID string) (*MultipartUpload, error) {
	if uploadID == "" {
		return nil, ErrInvalidUploadID
	}
	data := &MultipartUploadData{}
	err := m.store.GetMsg(ctx, []byte(uploadID), data)
	if err != nil {
		return nil, err
	}
	return &MultipartUpload{
		UploadID:        data.UploadId,
		Path:            data.Path,
		CreationDate:    data.CreationDate.AsTime(),
		PhysicalAddress: data.PhysicalAddress,
		Metadata:        data.Metadata,
		ContentType:     data.ContentType,
	}, nil
}

func (m *tracker) Delete(ctx context.Context, uploadID string) error {
	if uploadID == "" {
		return ErrInvalidUploadID
	}
	if _, err := m.Get(ctx, uploadID); err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return ErrMultipartUploadNotFound
		}
		return fmt.Errorf("failed on Get. key %s, %w", uploadID, err)
	}
	err := m.store.Delete(ctx, []byte(uploadID))
	return err
}
