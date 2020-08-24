package block

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

type MultipartTracker struct {
	DB db.Database
}

type MultipartPartInformation struct {
	Number int    `db:"number"`
	ETag   string `db:"etag"`
}

func (m *MultipartTracker) CreateMultipartUpload(ctx context.Context, storageNamespace string, uploadID string) error {
	_, err := m.DB.Transact(func(tx db.Tx) (interface{}, error) {
		return tx.Exec(`INSERT INTO block_multipart_uploads (storage_namespace,upload_id) VALUES ($1,$2)`,
			storageNamespace, uploadID)
	}, db.WithContext(ctx))
	return err
}

func (m *MultipartTracker) DeleteMultipartUpload(ctx context.Context, storageNamespace string, uploadID string) error {
	_, err := m.DB.Transact(func(tx db.Tx) (interface{}, error) {
		return tx.Exec(`DELETE FROM block_multipart_uploads WHERE storage_namespace=$1 AND upload_id=$2`,
			storageNamespace, uploadID)
	}, db.WithContext(ctx))
	return err
}

func (m *MultipartTracker) AddMultipartUploadPart(ctx context.Context, storageNamespace string, uploadID string, number int, etag string) error {
	_, err := m.DB.Transact(func(tx db.Tx) (interface{}, error) {
		return tx.Exec(`INSERT block_multipart_uploads_parts (storage_namespace,upload_id,number,etag) VALUES ($1,$2,$3,$4)`,
			storageNamespace, uploadID, number, etag)
	}, db.WithContext(ctx))
	return err
}

func (m *MultipartTracker) ListMultipartUploadParts(ctx context.Context, storageNamespace string, uploadID string) ([]MultipartPartInformation, error) {
	// TODO(barak): return sorted by part number
	res, err := m.DB.Transact(func(tx db.Tx) (interface{}, error) {
		var parts []MultipartPartInformation
		err := tx.Select(&parts, `SELECT number,etag FROM block_multipart_uploads_parts WHERE storage_namespace=$1 AND upload_id=$2`,
			storageNamespace, uploadID)
		return parts, err
	}, db.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	return res.([]MultipartPartInformation), nil
}
