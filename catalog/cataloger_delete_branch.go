package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) DeleteBranch(ctx context.Context, repository string, branch string) error {
	if err := Validate(ValidateFields{
		"repository": ValidateRepoName(repository),
		"branch":     ValidateBranchName(branch),
	}); err != nil {
		return err
	}

	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		// get repository id and default branch name
		var r struct {
			ID            string `db:"id"`
			DefaultBranch string `db:"default_branch"`
		}
		err := tx.Get(&r, `SELECT r.id, b.name as default_branch
			FROM repositories r
			JOIN branches b ON r.default_branch = b.id 
			WHERE r.name = $1`, repository)
		if err != nil {
			return nil, err
		}

		// fail in case we try to delete default branch
		if r.DefaultBranch == branch {
			return nil, ErrOperationNotPermitted
		}

		// check we don't have branch depends on us by count lineage records we are part of
		var ancestorCount int
		err = tx.Get(&ancestorCount, `SELECT count(branch_id) FROM lineage 
			WHERE ancestor_branch = (SELECT id FROM branches WHERE repository_id = $1 AND name = $2)`, r.ID, branch)
		if err != nil {
			return nil, err
		}
		if ancestorCount > 0 {
			return nil, ErrBranchHasDependentBranches
		}

		// delete branch entries
		_, err = tx.Exec(`DELETE FROM entries WHERE branch_id = (SELECT id FROM branches WHERE repository_id = $1 AND name = $2)`,
			r.ID, branch)
		if err != nil {
			return nil, err
		}

		// delete branch
		res, err := tx.Exec(`DELETE FROM branches WHERE repository_id = $1 AND name = $2`, r.ID, branch)
		if err != nil {
			return nil, err
		}
		if affected, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if affected != 1 {
			return nil, ErrBranchNotFound
		}
		return nil, nil
	}, c.txOpts(ctx)...)
	return err
}
