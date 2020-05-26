package catalog

import (
	"errors"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) CreateRepo(name string, bucket string, branchID int) (int, error) {
	if err := Validate(
		ValidateRepoName(name),
		ValidateBucketName(bucket),
		ValidateBranchID(branchID),
	); err != nil {
		return 0, err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		// make sure this repo doesn't already exist
		_, err := repoByName(tx, name)
		if err == nil {
			return nil, ErrRepoExists
		}
		if !errors.Is(err, db.ErrNotFound) {
			c.log.WithError(err).Error("could not read repo")
			return nil, err
		}

		creationDate := c.Clock.Now()
		repoID, err := repoInsert(tx, name, bucket, creationDate, branchID)
		if err != nil {
			return nil, err
		}

		return repoID, nil
	}, c.transactOpts()...)
	if err != nil {
		return 0, err
	}
	return res.(int), nil
}
