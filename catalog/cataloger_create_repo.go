package catalog

import (
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) CreateRepo(name string, bucket string, branch string) (int, error) {
	if err := Validate(
		ValidateRepoName(name),
		ValidateBucketName(bucket),
		ValidateBranchName(branch),
	); err != nil {
		return 0, err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		var branchID int64
		if err := tx.Get(&branchID, `SELECT nextval('branches_id_seq');`); err != nil {
			return nil, err
		}

		var repoID int64
		if err := tx.Get(&repoID, `SELECT nextval('repositories_id_seq');`); err != nil {
			return nil, err
		}

		creationDate := c.Clock.Now()
		sqlRepos, argsRepos := db.Builder.NewInsertBuilder().InsertInto("repositories").
			Cols("id", "name", "storage_namespace", "creation_date", "default_branch").
			Values(repoID, name, bucket, creationDate, branchID).
			Build()
		if _, err := tx.Exec(sqlRepos, argsRepos...); err != nil {
			return nil, err
		}

		sqlBranch, argsBranch := db.Builder.NewInsertBuilder().InsertInto("branches").
			Cols("repository_id", "id", "name").
			Values(repoID, branchID, branch).
			Build()
		if _, err := tx.Exec(sqlBranch, argsBranch...); err != nil {
			return nil, err
		}

		log.WithFields(logrus.Fields{
			"branch_id": branchID,
			"repo_id":   repoID,
		}).Debug("Repository created")
		return repoID, nil
	}, c.transactOpts()...)
	if err != nil {
		return 0, err
	}
	return int(res.(int64)), nil
}
