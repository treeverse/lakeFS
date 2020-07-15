package catalog

import (
	"context"
	"errors"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) DeleteEntry(ctx context.Context, repository, branch string, path string) error {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "branch", IsValid: ValidateBranchName(branch)},
		{Name: "path", IsValid: ValidatePath(path)},
	}); err != nil {
		return err
	}
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, branch)
		if err != nil {
			return nil, err
		}

		// delete uncommitted entry, if found first
		res, err := tx.Exec("DELETE FROM entries WHERE branch_id=$1 AND path=$2 AND min_commit=0 AND max_commit=max_commit_id()",
			branchID, path)
		if err != nil {
			return nil, err
		}
		deletedUncommittedCount, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}

		// get uncommitted entry based on path
		lineage, err := getLineage(tx, branchID, UncommittedID)
		if err != nil {
			return nil, err
		}
		sql, args, err := psql.
			Select("is_committed").
			FromSelect(sqEntriesLineage(branchID, UncommittedID, lineage), "entries").
			Where(sq.And{sq.Eq{"path": path}, sq.Eq{"is_deleted": false}}).
			ToSql()
		if err != nil {
			return nil, err
		}
		var isCommitted bool
		err = tx.Get(&isCommitted, sql, args...)
		committedNotFound := errors.Is(err, db.ErrNotFound)
		if err != nil && !committedNotFound {
			return nil, err
		}
		// 1. found committed record - add tombstone and return success
		// 2. not found committed record:
		//    - if we deleted uncommitted - return success
		//    - if we didn't delete uncommitted - return not found
		if isCommitted {
			_, err = tx.Exec(`INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata,min_commit,max_commit)
					VALUES ($1,$2,'','',0,'{}',0,0)`,
				branchID, path)
			return nil, err
		}
		if deletedUncommittedCount == 0 {
			return nil, ErrEntryNotFound
		}
		//		_, err = tx.Exec(`UPDATE entries
		//					SET physical_address=$3, checksum=$4, size=$5, metadata=$6, max_commit=$7
		//					WHERE branch_id=$1 AND path=$2 AND min_commit=0`,
		//			branchID, path, "", "", 0, nil, 0)
		//		return nil, err

		return nil, nil
	}, c.txOpts(ctx)...)
	return err
}
