package catalog

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

func (c *cataloger) CreateBranch(ctx context.Context, repo string, branch string, sourceBranch string) (*Branch, error) {
	if err := Validate(ValidateFields{
		"repo":         ValidateRepoName(repo),
		"branch":       ValidateBranchName(branch),
		"sourceBranch": ValidateBranchName(sourceBranch),
	}); err != nil {
		return nil, err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		// next id for branch
		var branchID int
		if err := tx.Get(&branchID, `SELECT nextval('branches_id_seq');`); err != nil {
			return nil, err
		}

		// get repo id by name
		repoDB, err := repoGetByName(tx, repo)
		if err != nil {
			return nil, err
		}

		// get source branch
		b := db.Builder.NewSelectBuilder()
		sql, args := b.From("branches").
			Select("id").
			Where(b.And(b.Equal("repository_id", repoDB.ID), b.Equal("name", sourceBranch))).
			Build()
		var sourceBranchID int
		if err := tx.Get(&sourceBranchID, sql, args...); err != nil {
			return nil, err
		}

		// insert new lineage for the new branch
		res, err := tx.Exec(`INSERT INTO lineage
			SELECT branch_id, precedence, ancestor_branch, effective_commit
			FROM lineage_v
			WHERE branch_id = $1
			LIMIT 1`, sourceBranchID)
		if err != nil {
			return nil, err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		if affected != 1 {
			return nil, fmt.Errorf("lineage not found for source branch id: %d", sourceBranchID)
		}

		// insert new branch
		sqlBranch, argsBranch := db.Builder.
			NewInsertBuilder().
			InsertInto("branches").
			Cols("repository_id", "id", "name").
			Values(repoDB.ID, branchID, branch).
			Build()
		_, err = tx.Exec(sqlBranch, argsBranch...)
		if err != nil {
			return nil, err
		}
		newBranch := &Branch{
			RepositoryID: repoDB.ID,
			ID:           int(branchID),
			Name:         branch,
			//NextCommit:
		}

		c.log.WithContext(ctx).
			WithFields(logging.Fields{
				"repo":             repo,
				"branch_id":        branchID,
				"branch":           branch,
				"repo_id":          repoDB.ID,
				"source_branch":    sourceBranch,
				"source_branch_id": sourceBranchID,
			}).Debug("Branch created")
		return newBranch, nil
	}, c.transactOpts(ctx)...)
	if err != nil {
		return nil, err
	}
	return res.(*Branch), nil
}
