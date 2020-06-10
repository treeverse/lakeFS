package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) DeleteMultipartUpload(ctx context.Context, repository string, uploadID string) error {
	if err := Validate(ValidateFields{
		"repository": ValidateRepoName(repository),
		"uploadID":   ValidateUploadID(uploadID),
	}); err != nil {
		return err
	}

	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		repoID, err := getRepoID(tx, repository)
		if err != nil {
			return nil, err
		}
		res, err := tx.Exec(`DELETE FROM multipart_uploads WHERE repository_id = $1 AND upload_id = $2`,
			repoID, uploadID)
		if err != nil {
			return nil, err
		}
		if affected, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if affected != 1 {
			return nil, ErrMultipartUploadNotFound
		}
		return nil, nil
	}, c.txOpts(ctx)...)
	return err
}
