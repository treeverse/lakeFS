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

		// delete uncommitted entry on branch if found
		res, err := tx.Exec(`DELETE FROM entries_v WHERE branch_id = $1 AND path = $2 AND NOT is_committed AND NOT is_deleted`,
			branchID, path)
		if err != nil {
			return nil, err
		}
		deletedUncommitted, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		// read previously committed entry
		// TODO(barak): does the tombstone needs to reference the previous entry information?
		var ent Entry
		err = tx.Get(&ent, `SELECT source_branch as branch_id,physical_address,checksum,size,metadata
			FROM entries_lineage_committed_v
			WHERE displayed_branch = $1 AND path = $2 AND NOT is_deleted`, branchID, path)
		if errors.Is(err, db.ErrNotFound) {
			// no previous entry - if we deleted uncommitted entry - success else - not found
			if deletedUncommitted > 0 {
				return nil, nil
			}
			return nil, ErrEntryNotFound
		}
		if err != nil {
			return nil, err
		}

		// committed entry found - make sure we have tombstone entry
		res, err = tx.Exec(`INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata,min_commit,max_commit) values ($1,$2,$3,$4,$5,$6,$7,$8)`,
			branchID, path, ent.PhysicalAddress, ent.Checksum, ent.Size, ent.Metadata, 0, 0)
		if err != nil {
			return nil, err
		}
		if affected, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if affected != 1 {
			return nil, ErrEntryUpdateFailed
		}
		return nil, nil
	}, c.txOpts(ctx)...)
	return err
}
