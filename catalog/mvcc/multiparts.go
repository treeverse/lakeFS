package mvcc

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

type multiparts struct {
	db db.Database
}

func NewMultiparts(adb db.Database) catalog.Multiparts {
	return &multiparts{
		db: adb,
	}
}

func (m *multiparts) Create(ctx context.Context, uploadID, path, physicalAddress string, creationTime time.Time) error {
	if err := Validate(ValidateFields{
		{Name: "uploadID", IsValid: ValidateUploadID(uploadID)},
		{Name: "path", IsValid: ValidatePath(path)},
		{Name: "physicalAddress", IsValid: ValidatePhysicalAddress(physicalAddress)},
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

func (m *multiparts) Get(ctx context.Context, uploadID string) (*catalog.MultipartUpload, error) {
	if err := Validate(ValidateFields{
		{Name: "uploadID", IsValid: ValidateUploadID(uploadID)},
	}); err != nil {
		return nil, err
	}

	res, err := m.db.Transact(func(tx db.Tx) (interface{}, error) {
		var m catalog.MultipartUpload
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
	return res.(*catalog.MultipartUpload), nil
}

func (m *multiparts) Delete(ctx context.Context, uploadID string) error {
	if err := Validate(ValidateFields{
		{Name: "uploadID", IsValid: ValidateUploadID(uploadID)},
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
			return nil, catalog.ErrMultipartUploadNotFound
		}
		return nil, nil
	})
	return err
}
