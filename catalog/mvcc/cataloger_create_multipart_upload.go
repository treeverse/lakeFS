package mvcc

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) CreateMultipartUpload(ctx context.Context, repository string, uploadID, path, physicalAddress string, creationTime time.Time) error {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "uploadID", IsValid: ValidateUploadID(uploadID)},
		{Name: "path", IsValid: ValidatePath(path)},
		{Name: "physicalAddress", IsValid: ValidatePhysicalAddress(physicalAddress)},
	}); err != nil {
		return err
	}

	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		repoID, err := c.getRepositoryIDCache(tx, repository)
		if err != nil {
			return nil, err
		}
		_, err = tx.Exec(`INSERT INTO catalog_multipart_uploads (repository_id,upload_id,path,creation_date,physical_address)
			VALUES ($1, $2, $3, $4, $5)`,
			repoID, uploadID, path, creationTime, physicalAddress)
		return nil, err
	}, c.txOpts(ctx)...)
	return err
}
