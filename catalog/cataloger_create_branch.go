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
		repoID, err := repoGetIDByName(tx, repo)
		if err != nil {
			return nil, err
		}

		// get source branch id
		var sourceBranchID int
		if err := tx.Get(&sourceBranchID, `SELECT id FROM branches WHERE repository_id = $1 AND name = $2`, repoID, sourceBranch); err != nil {
			return nil, err
		}

		// next id for branch
		var branchID int
		if err := tx.Get(&branchID, `SELECT nextval('branches_id_seq');`); err != nil {
			return nil, err
		}

		// insert new branch
		if _, err := tx.Exec(`INSERT INTO branches (repository_id, id, name) VALUES($1, $2, $3)`, repoID, branchID, branch); err != nil {
			return nil, err
		}

		// insert new lineage for the new branch
		res, err := tx.Exec(`INSERT INTO lineage (branch_id, precedence, ancestor_branch, effective_commit)
			SELECT $1, precedence + 1, ancestor_branch, effective_commit
			FROM lineage_v
			WHERE branch_id = $2`, branchID, sourceBranchID)
		if err != nil {
			return nil, err
		}
		if affected, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if affected == 0 {
			return nil, fmt.Errorf("lineage not found for source branch id: %d", sourceBranchID)
		}

		c.log.WithContext(ctx).
			WithFields(logging.Fields{"repo": repo, "branch_id": branchID, "branch": branch, "repo_id": repoID, "source_branch": sourceBranch, "source_branch_id": sourceBranchID}).
			Debug("Branch created")
		return &Branch{
			RepositoryID: repoID,
			ID:           branchID,
			Name:         branch,
			NextCommit:   1,
		}, nil
	}, c.txOpts(ctx)...)
	if err != nil {
		return nil, err
	}
	return res.(*Branch), nil
}
