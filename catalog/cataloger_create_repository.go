package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

func (c *cataloger) CreateRepository(ctx context.Context, repository string, bucket string, branch string) error {
	if err := Validate(ValidateFields{
		"repository": ValidateRepositoryName(repository),
		"bucket":     ValidateBucketName(bucket),
		"branch":     ValidateBranchName(branch),
	}); err != nil {
		return err
	}

	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		// next id for branch
		var branchID int
		if err := tx.Get(&branchID, `SELECT nextval('branches_id_seq')`); err != nil {
			return nil, err
		}

		// next id for repository
		var repoID int
		if err := tx.Get(&repoID, `SELECT nextval('repositories_id_seq')`); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(`SET CONSTRAINTS repositories_branches_id_fkey DEFERRED`); err != nil {
			return nil, err
		}
		// create repository with ref to branch
		creationDate := c.Clock.Now()
		if _, err := tx.Exec(`INSERT INTO repositories (id, name, storage_namespace, creation_date, default_branch)
			VALUES ($1, $2, $3, $4, $5)`, repoID, repository, bucket, creationDate, branchID); err != nil {
			return nil, err
		}
		// create branch with ref to repository
		if _, err := tx.Exec(`INSERT INTO branches (repository_id, id, name)
			VALUES ($1, $2, $3)`, repoID, branchID, branch); err != nil {
			return nil, err
		}

		c.log.WithContext(ctx).
			WithFields(logging.Fields{"branch_id": branchID, "branch": branch, "repo_id": repoID, "repository": repository}).
			Debug("Repository created")
		return repoID, nil
	}, c.txOpts(ctx)...)
	return err
}
