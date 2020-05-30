package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

func (c *cataloger) CreateRepo(ctx context.Context, repo string, bucket string, branch string) error {
	if err := Validate(ValidateFields{
		"repo":   ValidateRepoName(repo),
		"bucket": ValidateBucketName(bucket),
		"branch": ValidateBranchName(branch),
	}); err != nil {
		return err
	}

	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		// next id for branch
		var branchID int64
		if err := tx.Get(&branchID, `SELECT nextval('branches_id_seq');`); err != nil {
			return nil, err
		}

		// next id for repository
		var repoID int64
		if err := tx.Get(&repoID, `SELECT nextval('repositories_id_seq');`); err != nil {
			return nil, err
		}

		// create repository with ref to branch
		creationDate := c.Clock.Now()
		sqlRepos, argsRepos := db.Builder.
			NewInsertBuilder().
			InsertInto("repositories").
			Cols("id", "name", "storage_namespace", "creation_date", "default_branch").
			Values(repoID, repo, bucket, creationDate, branchID).
			Build()
		if _, err := tx.Exec(sqlRepos, argsRepos...); err != nil {
			return nil, err
		}

		// create branch with ref to repository
		sqlBranch, argsBranch := db.Builder.
			NewInsertBuilder().
			InsertInto("branches").
			Cols("repository_id", "id", "name").
			Values(repoID, branchID, branch).
			Build()
		if _, err := tx.Exec(sqlBranch, argsBranch...); err != nil {
			return nil, err
		}

		c.log.WithContext(ctx).
			WithFields(logging.Fields{
				"branch_id": branchID,
				"branch":    branch,
				"repo_id":   repoID,
				"repo":      repo,
			}).Debug("Repository created")
		return repoID, nil
	}, c.transactOpts(ctx)...)
	return err
}
