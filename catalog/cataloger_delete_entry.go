package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) DeleteEntry(ctx context.Context, repository, branch string, path string) error {
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

		// getting two entries on this path so we can check uncommitted and committed changes
		var entries []*Entry
		err = tx.Select(&entries, `SELECT source_branch as branch_id,physical_address,checksum,size,metadata,min_commit,max_commit,is_tombstone
			FROM entries_lineage_full_v
			WHERE displayed_branch = $1 AND path = $2 ORDER BY rank LIMIT 2
		`, branchID, path)
		if err != nil {
			return nil, err
		}
		// handle the case where there is no entry to delete
		if len(entries) == 0 || entries[0].IsTombstone {
			return nil, ErrEntryNotFound
		}

		switch {
		case entries[0].MinCommit == 0 && len(entries) == 2:
			// uncommitted change with previous committed entry - update to tombstone
			_, err = tx.Exec(`UPDATE entries SET physical_address=$3, checksum=$4, size=$5, metadata=$6, max_commit=$7
				WHERE displayed_branch = $1 AND path = $2 AND min_commit = 0`,
				branchID, path, "", "", 0, nil, 0)
		case entries[0].MinCommit == 0 && len(entries) == 1:
			// uncommitted change without previous committed entry - delete uncommitted
			_, err = tx.Exec(`DELETE FROM entries_v WHERE branch_id = $1 AND path = $2 AND NOT is_committed`,
				branchID, path)
		case entries[0].MinCommit != 0:
			// committed change - add tombstone
			ent := entries[0]
			_, err = tx.Exec(`INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata,min_commit,max_commit) 
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
				branchID, path, ent.PhysicalAddress, ent.Checksum, ent.Size, ent.Metadata, 0, 0)
		default:
			return nil, ErrInvalidState
		}
		return nil, err
	}, c.txOpts(ctx)...)
	return err
}
