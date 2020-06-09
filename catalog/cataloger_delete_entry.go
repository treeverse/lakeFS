package catalog

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) DeleteEntry(ctx context.Context, repository string, branch string, path string) error {
	if err := Validate(ValidateFields{
		"repository": ValidateRepoName(repository),
		"branch":     ValidateBranchName(branch),
		"path":       ValidatePath(path),
	}); err != nil {
		return err
	}
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repository, branch, LockTypeShare)
		if err != nil {
			return nil, err
		}

		// read previously committed entry
		committedEntryFound := true
		var ent Entry
		err = tx.Get(&ent, `SELECT source_branch as branch_id,physical_address,checksum,size,metadata
			FROM entries_lineage_committed_v
			WHERE displayed_branch = $1 AND path = $2 AND NOT is_deleted`, branchID, path)
		if errors.Is(err, db.ErrNotFound) {
			committedEntryFound = false
		} else if err != nil {
			return nil, err
		}

		if committedEntryFound {
			// make uncommitted tombstone for this entry
			res, err := tx.Exec(`INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata,min_commit,max_commit) 
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
				ON CONFLICT (branch_id,path,min_commit)
				DO UPDATE SET physical_address=$3, checksum=$4, size=$5, metadata=$6, min_commit=$7, max_commit=$8`,
				branchID, path, ent.PhysicalAddress, ent.Checksum, ent.Size, ent.Metadata, 0, 0)
			if err != nil {
				return nil, err
			}
			if affected, err := res.RowsAffected(); err != nil {
				return nil, err
			} else if affected != 1 {
				return nil, ErrEntryUpdateFailed
			}
		} else {
			// delete uncommitted entry on branch if found
			res, err := tx.Exec(`DELETE FROM entries_v WHERE branch_id = $1 AND path = $2 AND NOT is_committed AND NOT is_deleted`,
				branchID, path)
			if err != nil {
				return nil, err
			}
			if deletedUncommitted, err := res.RowsAffected(); err != nil {
				return nil, err
			} else if deletedUncommitted == 0 {
				return nil, ErrEntryNotFound
			}
		}
		return nil, nil
	}, c.txOpts(ctx)...)
	return err
}
