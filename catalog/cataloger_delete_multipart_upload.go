package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) DeleteMultipartUpload(ctx context.Context, repo string, uploadID string) error {
	if err := Validate(ValidateFields{
		"repo":     ValidateRepoName(repo),
		"uploadID": ValidateUploadID(uploadID),
	}); err != nil {
		return err
	}

	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		repoID, err := repoGetIDByName(tx, repo)
		if err != nil {
			return nil, err
		}
		res, err := tx.Exec(`DELETE FROM multipart_uploads
			WHERE repository_id = $1 AND upload_id = $2`,
			repoID, uploadID)
		if err != nil {
			return nil, err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		if affected != 1 {
			return nil, ErrMultipartUploadNotFound
		}
		return nil, nil
	}, c.transactOpts(ctx)...)
	return err
}
