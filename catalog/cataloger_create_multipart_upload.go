package catalog

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) CreateMultipartUpload(ctx context.Context, repo string, uploadID, path, physicalAddress string, creationTime time.Time) error {
	if err := Validate(ValidateFields{
		"repo":            ValidateRepoName(repo),
		"uploadID":        ValidateUploadID(uploadID),
		"path":            ValidatePath(path),
		"physicalAddress": ValidatePhysicalAddress(physicalAddress),
	}); err != nil {
		return err
	}

	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		repoID, err := repoGetIDByName(tx, repo)
		if err != nil {
			return nil, err
		}
		_, err = tx.Exec(`INSERT INTO multipart_uploads (repository_id, upload_id, path, creation_date, physical_address)
			VALUES ($1, $2, $3, $4, $5)`,
			repoID, uploadID, path, creationTime, physicalAddress)
		return nil, err
	}, c.txOpts(ctx)...)
	return err
}
