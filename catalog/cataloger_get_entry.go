package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) GetEntry(ctx context.Context, repository, branch string, commitID CommitID, path string) (*Entry, error) {
	if err := Validate(ValidateFields{
		"repository": ValidateRepoName(repository),
		"branch":     ValidateBranchName(branch),
		"path":       ValidatePath(path),
	}); err != nil {
		return nil, err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repository, branch, LockTypeNone)
		if err != nil {
			return nil, err
		}

		var q string
		var args []interface{}
		switch commitID {
		case CommittedID:
			q = `SELECT displayed_branch as branch_id, path, physical_address, creation_date, size, checksum, metadata, min_commit, max_commit, is_tombstone
					FROM entries_lineage_committed_v
					WHERE displayed_branch = $1 AND path = $2 AND NOT is_deleted`
			args = []interface{}{branchID, path}
		case UncommittedID:
			q = `SELECT displayed_branch as branch_id, path, physical_address, creation_date, size, checksum, metadata, min_commit, max_commit, is_tombstone
					FROM entries_lineage_v
					WHERE displayed_branch = $1 AND path = $2 AND NOT is_deleted`
			args = []interface{}{branchID, path}
		default:
			q = `SELECT displayed_branch as branch_id, path, physical_address, creation_date, size, checksum, metadata, min_commit, max_commit, is_tombstone
	FROM (SELECT l.branch_id AS displayed_branch,
    e.branch_id AS source_branch,
    e.path,
    e.min_commit,
        CASE
            WHEN l.main_branch THEN e.max_commit
            WHEN (e.max_commit <= l.effective_commit) THEN e.max_commit
            ELSE ('01111111111111111111111111111111'::"bit")::INTEGER
        END AS max_commit,
    e.physical_address,
    e.creation_date,
    e.size,
    e.checksum,
    e.metadata,
    l.precedence,
    row_number() OVER (PARTITION BY l.branch_id, e.path ORDER BY l.precedence,
        CASE
            WHEN (l.main_branch AND (e.min_commit = 0)) THEN ('01111111111111111111111111111111'::"bit")::INTEGER
            ELSE e.min_commit
        END DESC) AS rank,
    l.min_commit AS branch_min_commit,
    l.max_commit AS branch_max_commit,
    e.is_committed,
        CASE
            WHEN l.main_branch THEN e.is_deleted
            ELSE (e.max_commit <= l.effective_commit)
        END AS is_deleted,
    l.active_lineage,
    l.effective_commit,
    e.is_tombstone
   FROM (entries_v e
     JOIN lineage_v l ON ((l.ancestor_branch = e.branch_id)))
  WHERE ((l.main_branch OR ((e.min_commit <= l.effective_commit) AND e.is_committed)) AND l.active_lineage) 
    AND e.is_committed AND l.branch_id=$1 AND e.path=$2 AND e.min_commit<=$3 AND e.max_commit>=$3
) t
WHERE rank=1;`
			args = []interface{}{branchID, path, commitID}
		}
		var ent Entry
		if err := tx.Get(&ent, q, args...); err != nil {
			return nil, err
		}
		return &ent, nil
	}, c.txOpts(ctx, db.ReadOnly())...)
	if err != nil {
		return nil, err
	}
	return res.(*Entry), nil
}
