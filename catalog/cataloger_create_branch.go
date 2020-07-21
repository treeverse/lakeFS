package catalog

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/db"
)

const (
	createBranchCommitMessage = "Branch created"
)

func (c *cataloger) CreateBranch(ctx context.Context, repository, branch string, sourceBranch string) error {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "branch", IsValid: ValidateBranchName(branch)},
		{Name: "sourceBranch", IsValid: ValidateBranchName(sourceBranch)},
	}); err != nil {
		return err
	}

	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		repoID, err := c.getRepositoryIDCache(tx, repository)
		if err != nil {
			return nil, err
		}

		// get source branch id and
		var sourceBranchID int
		if err := tx.Get(&sourceBranchID, `SELECT id FROM branches WHERE repository_id=$1 AND name=$2`,
			repoID, sourceBranch); err != nil {
			return nil, fmt.Errorf("source branch id: %w", err)
		}

		// next id for branch
		var branchID int64
		if err := tx.Get(&branchID, `SELECT nextval('branches_id_seq')`); err != nil {
			return nil, fmt.Errorf("next branch id: %w", err)
		}

		// insert new branch
		if _, err := tx.Exec(`INSERT INTO branches (repository_id, id, name, lineage)
			VALUES($1,$2,$3,(SELECT $4::bigint||lineage FROM branches WHERE id=$4))`,
			repoID, branchID, branch, sourceBranchID); err != nil {
			return nil, fmt.Errorf("insert branch: %w", err)
		}

		// create initial commit
		creationDate := c.clock.Now()

		_, err = tx.Exec(`INSERT INTO commits (branch_id, commit_id, previous_commit_id,committer, message,
							creation_date,merge_source_branch, merge_type, lineage_commits,merge_source_commit)
			VALUES ($1,nextval('commit_id_seq'),0,$2,$3,$4,$5,'from_father',
				(select (select max(commit_id) from commits where branch_id=$5)|| 
					(select distinct on (branch_id) lineage_commits from commits 
						where branch_id=$5 and merge_type='from_father' order by branch_id,commit_id desc))
						,(select max(commit_id) from commits where branch_id=$5 ))`,
			branchID, CatalogerCommitter, createBranchCommitMessage, creationDate, sourceBranchID)
		if err != nil {
			return nil, fmt.Errorf("insert commit: %w", err)
		}
		return branchID, nil
	}, c.txOpts(ctx)...)
	return err
}
