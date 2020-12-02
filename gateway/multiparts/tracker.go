package multiparts

import (
	"context"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/catalog/mvcc"
	"github.com/treeverse/lakefs/db"
)

type MultipartUpload struct {
	UploadID        string    `db:"upload_id"`
	Path            string    `db:"path"`
	CreationDate    time.Time `db:"creation_date"`
	PhysicalAddress string    `db:"physical_address"`
}

type Tracker interface {
	Create(ctx context.Context, uploadID, path, physicalAddress string, creationTime time.Time) error
	Get(ctx context.Context, uploadID string) (*MultipartUpload, error)
	Delete(ctx context.Context, uploadID string) error
}

type tracker struct {
	db db.Database
}

var ErrMultipartUploadNotFound = fmt.Errorf("multipart upload %w", db.ErrNotFound)

func NewTracker(adb db.Database) Tracker {
	return &tracker{
		db: adb,
	}
}

func (m *tracker) Create(ctx context.Context, uploadID, path, physicalAddress string, creationTime time.Time) error {
	if err := mvcc.Validate(mvcc.ValidateFields{
		{Name: "uploadID", IsValid: mvcc.ValidateUploadID(uploadID)},
		{Name: "path", IsValid: mvcc.ValidatePath(path)},
		{Name: "physicalAddress", IsValid: mvcc.ValidatePhysicalAddress(physicalAddress)},
	}); err != nil {
		return err
	}

	_, err := m.db.Transact(func(tx db.Tx) (interface{}, error) {
		_, err := tx.Exec(`INSERT INTO gateway_multiparts (upload_id,path,creation_date,physical_address)
			VALUES ($1, $2, $3, $4)`,
			uploadID, path, creationTime, physicalAddress)
		return nil, err
	}, db.WithContext(ctx))
	return err
}

func (m *tracker) Get(ctx context.Context, uploadID string) (*MultipartUpload, error) {
	if err := mvcc.Validate(mvcc.ValidateFields{
		{Name: "uploadID", IsValid: mvcc.ValidateUploadID(uploadID)},
	}); err != nil {
		return nil, err
	}

	res, err := m.db.Transact(func(tx db.Tx) (interface{}, error) {
		var m MultipartUpload
		if err := tx.Get(&m, `
			SELECT upload_id, path, creation_date, physical_address 
			FROM gateway_multiparts
			WHERE upload_id = $1`,
			uploadID); err != nil {
			return nil, err
		}
		return &m, nil
	}, db.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	return res.(*MultipartUpload), nil
}

func (m *tracker) Delete(ctx context.Context, uploadID string) error {
	if err := mvcc.Validate(mvcc.ValidateFields{
		{Name: "uploadID", IsValid: mvcc.ValidateUploadID(uploadID)},
	}); err != nil {
		return err
	}

	_, err := m.db.Transact(func(tx db.Tx) (interface{}, error) {
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
