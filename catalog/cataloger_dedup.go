package catalog

import (
	"context"

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

		// add dedup record and return on success
		res, err := tx.Exec(`INSERT INTO object_dedup (repository_id, dedup_id, physical_address) values ($1, decode($2,'hex'), $3)
			ON CONFLICT DO NOTHING`,
			repoID, dedupID, physicalAddress)
		if err != nil {
			return nil, err
		}
		if rowsAffected, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if rowsAffected == 1 {
			return physicalAddress, nil
		}

		var addr string
		err = tx.Get(&addr, `SELECT physical_address FROM object_dedup WHERE repository_id=$1 AND dedup_id=decode($2,'hex')`,
			repoID, dedupID)
		if err != nil {
			return nil, err
		}
		return addr, nil
	}, c.txOpts(ctx)...)
	if err != nil {
		return "", err
	}
	return addr.(string), nil
}
