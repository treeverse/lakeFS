package multiparts

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/db"
)

type Metadata map[string]string

type MultipartUpload struct {
	UploadID        string    `db:"upload_id"`
	Path            string    `db:"path"`
	CreationDate    time.Time `db:"creation_date"`
	PhysicalAddress string    `db:"physical_address"`
	Metadata        Metadata  `db:"metadata"`
	ContentType     string    `db:"content_type"`
}

type Tracker interface {
	Create(ctx context.Context, multipart MultipartUpload) error
	Get(ctx context.Context, uploadID string) (*MultipartUpload, error)
	Delete(ctx context.Context, uploadID string) error
}

type tracker struct {
	db db.Database
}

var (
	ErrMultipartUploadNotFound  = errors.New("multipart upload not found")
	ErrInvalidUploadID          = errors.New("invalid upload id")
	ErrInvalidMetadataSrcFormat = errors.New("invalid metadata source format")
)

func NewTracker(adb db.Database) Tracker {
	return &tracker{
		db: adb,
	}
}

func (m *tracker) Create(ctx context.Context, multipart MultipartUpload) error {
	if multipart.UploadID == "" {
		return ErrInvalidUploadID
	}
	_, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		_, err := tx.Exec(`INSERT INTO gateway_multiparts (upload_id,path,creation_date,physical_address, metadata, content_type)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			multipart.UploadID, multipart.Path, multipart.CreationDate, multipart.PhysicalAddress, multipart.Metadata, multipart.ContentType)
		return nil, err
	})
	return err
}

func (m *tracker) Get(ctx context.Context, uploadID string) (*MultipartUpload, error) {
	if uploadID == "" {
		return nil, ErrInvalidUploadID
	}
	res, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		var m MultipartUpload
		if err := tx.Get(&m, `
			SELECT upload_id, path, creation_date, physical_address, metadata, content_type 
			FROM gateway_multiparts
			WHERE upload_id = $1`,
			uploadID); err != nil {
			return nil, err
		}
		return &m, nil
	})
	if err != nil {
		return nil, err
	}
	return res.(*MultipartUpload), nil
}

func (m *tracker) Delete(ctx context.Context, uploadID string) error {
	if uploadID == "" {
		return ErrInvalidUploadID
	}
	_, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		res, err := tx.Exec(`DELETE FROM gateway_multiparts WHERE upload_id = $1`, uploadID)
		if err != nil {
			return nil, err
		}
		affected := res.RowsAffected()
		if affected != 1 {
			return nil, ErrMultipartUploadNotFound
		}
		return nil, nil
	})
	return err
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
