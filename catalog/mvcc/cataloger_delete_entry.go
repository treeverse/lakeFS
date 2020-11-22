package mvcc

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) DeleteEntry(ctx context.Context, repository, branch string, path string) error {
	if err := catalog.Validate(catalog.ValidateFields{
		{Name: "repository", IsValid: catalog.ValidateRepositoryName(repository)},
		{Name: "branch", IsValid: catalog.ValidateBranchName(branch)},
	}); err != nil {
		return err
	}
	if path == "" {
		return db.ErrNotFound
	}
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, branch)
		if err != nil {
			return nil, err
		}

		// delete uncommitted entry, if found first
		res, err := tx.Exec("DELETE FROM catalog_entries WHERE branch_id=$1 AND path=$2 AND min_commit=$3 AND max_commit=$4",
			branchID, path, catalog.MinCommitUncommittedIndicator, catalog.MaxCommitID)
		if err != nil {
			return nil, fmt.Errorf("uncommitted: %w", err)
		}
		deletedUncommittedCount := res.RowsAffected()

		// get uncommitted entry based on path
		lineage, err := getLineage(tx, branchID, catalog.UncommittedID)
		if err != nil {
			return nil, fmt.Errorf("get lineage: %w", err)
		}
		sql, args, err := psql.
			Select("is_committed").
			FromSelect(sqEntriesLineage(branchID, catalog.UncommittedID, lineage), "entries").
			// Expired objects *can* be successfully deleted!
			Where(sq.Eq{"path": path, "is_deleted": false}).
			ToSql()
		if err != nil {
			return nil, fmt.Errorf("build sql: %w", err)
		}
		var isCommitted bool
		err = tx.GetPrimitive(&isCommitted, sql, args...)
		committedNotFound := errors.Is(err, db.ErrNotFound)
		if err != nil && !committedNotFound {
			return nil, err
		}
		// 1. found committed record - add tombstone and return success
		// 2. not found committed record:
		//    - if we deleted uncommitted - return success
		//    - if we didn't delete uncommitted - return not found
		if isCommitted {
			_, err = tx.Exec(`INSERT INTO catalog_entries (branch_id,path,physical_address,checksum,size,metadata,min_commit,max_commit)
					VALUES ($1,$2,'','',0,'{}',$3,0)`,
				branchID, path, catalog.MaxCommitID)
			if err != nil {
				return nil, fmt.Errorf("tombstone: %w", err)
			}
			return nil, nil
		}
		if deletedUncommittedCount == 0 {
			return nil, catalog.ErrEntryNotFound
		}
		return nil, nil
	}, c.txOpts(ctx)...)
	return err
}
