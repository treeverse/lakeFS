package catalog

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) DeleteEntry(ctx context.Context, repo string, branch string, path string) error {
	if err := Validate(ValidateFields{
		"repo":   ValidateRepoName(repo),
		"branch": ValidateBranchName(branch),
		"path":   ValidatePath(path),
	}); err != nil {
		return err
	}
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repo, branch, LockTypeShare)
		if err != nil {
			return nil, err
		}

		// make sure no prefix non committed entry is found
		res, err := tx.Exec(`DELETE FROM entries_v WHERE branch_id = $1 AND path = $2 AND NOT is_committed`,
			branchID, path)
		if err != nil {
			return nil, err
		}
		if affected, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if affected == 1 {
			// deleted uncommitted entry
			return nil, nil
		}

		// TODO(barak): metadata is missing

		// if we have a committed non-deleted record we should insert a new working record flagged as deleted
		// TODO(barak): does the tombstone needs to reference the previous entry information?
		var ent Entry
		err = tx.Get(&ent, `SELECT physical_address,checksum,size 
			FROM entries_v 
			WHERE branch_id = $1 AND path = $2 AND is_committed`, branchID, path)
		if errors.Is(err, db.ErrNotFound) {
			// no previous entry - file not found
			return nil, ErrEntryNotFound
		}
		if err != nil {
			return nil, err
		}

		// TODO(barak): metadata is missing
		// insert tombstone
		res, err = tx.Exec(`INSERT INTO entries (branch_id,path,physical_address,checksum,size,min_commit,max_commit) values ($1,$2,$3,$4,$5,$6,$7)`,
			branchID, path, ent.PhysicalAddress, ent.Checksum, ent.Size, 0, 0)
		if err != nil {
			return nil, err
		}
		if affected, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if affected != 1 {
			return nil, ErrEntryUpdateFailed
		}
		return nil, nil
	})
	return err
}
