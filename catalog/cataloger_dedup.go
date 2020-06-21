package catalog

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) Dedup(ctx context.Context, repository string, dedupID string, physicalAddress string) (string, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "dedupID", IsValid: ValidateDedupID(dedupID)},
		{Name: "physicalAddress", IsValid: ValidatePhysicalAddress(physicalAddress)},
	}); err != nil {
		return physicalAddress, err
	}

	addr, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		repoID, err := getRepositoryID(tx, repository)
		if err != nil {
			return physicalAddress, err
		}

		var dedupAddress string
		err = tx.Get(&dedupAddress, `SELECT physical_address
			FROM object_dedup
			WHERE repository_id=$1 AND dedup_id=decode($2,'hex')`,
			repoID, dedupID)
		if err == nil {
			return dedupAddress, nil
		}
		if !errors.Is(err, db.ErrNotFound) {
			return physicalAddress, err
		}

		// add dedup record in case of not found
		_, err = tx.Exec(`INSERT INTO object_dedup (repository_id, dedup_id, physical_address) values ($1, decode($2,'hex'), $3)`,
			repoID, dedupID, physicalAddress)

		return physicalAddress, err
	}, c.txOpts(ctx)...)
	if err != nil {
		return "", err
	}
	return addr.(string), nil
}
