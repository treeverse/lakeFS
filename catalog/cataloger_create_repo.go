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
		_, err := tx.Exec(`
			INSERT INTO repositories (id,name, storage_namespace, creation_date, default_branch)
			VALUES ($1,$2,$3,$4,$5)`,
			repoID, name, bucket, creationDate, branchID)
		if err != nil {
			return nil, err
		}
		_, err = tx.Exec(`INSERT INTO branches (repository_id, id, name) VALUES ($1,$2,$3)`,
			repoID, branchID, branch)
		if err != nil {
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
