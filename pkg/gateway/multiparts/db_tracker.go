package multiparts

import (
	"context"

	"github.com/treeverse/lakefs/pkg/db"
)

type dbTracker struct {
	db db.Database
}

func NewDBTracker(adb db.Database) Tracker {
	return &dbTracker{
		db: adb,
	}
}

func (m *dbTracker) Create(ctx context.Context, multipart MultipartUpload) error {
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

func (m *dbTracker) Get(ctx context.Context, uploadID string) (*MultipartUpload, error) {
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

func (m *dbTracker) Delete(ctx context.Context, uploadID string) error {
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
