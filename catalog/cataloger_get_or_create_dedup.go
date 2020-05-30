package catalog

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) GetOrCreateDedup(ctx context.Context, repo string, dedupID string, physicalAddress string) (string, error) {
	if err := Validate(ValidateFields{
		"repo":            ValidateRepoName(repo),
		"dedupID":         ValidateDedupID(dedupID),
		"physicalAddress": ValidateNonEmptyString(physicalAddress),
	}); err != nil {
		return physicalAddress, err
	}

	// TODO(barak): do we need to return physical address all the time?
	addr, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		var repoID int
		if err := tx.Get(&repoID, `SELECT id FROM repositories WHERE name=$1`, repo); err != nil {
			return physicalAddress, ErrRepoNotFound
		}

		var od ObjectDedup
		err := tx.Get(&od, `SELECT repository_id, encode(dedup_id,'hex') as dedup_id, physical_address
			FROM object_dedup
			WHERE repository_id=$1 AND dedup_id=decode($2,'hex')`,
			repoID, dedupID)
		if err == nil {
			return od.PhysicalAddress, nil
		}
		if !errors.Is(err, db.ErrNotFound) {
			return physicalAddress, err
		}

		// TODO(barak): how do we plan to pass size and parts?
		_, err = tx.Exec(`INSERT INTO object_dedup (repository_id, dedup_id, physical_address, size) values ($1, decode($2,'hex'), $3, 0)`,
			repoID, dedupID, physicalAddress)

		return physicalAddress, err
	}, c.transactOpts(ctx)...)
	if err != nil {
		return "", err
	}
	return addr.(string), nil
}
