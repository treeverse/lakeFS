package catalog

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/db"
)

const (
	createRepositoryCommitMessage = "Repository created"
)

func (c *cataloger) CreateRepository(ctx context.Context, repository string, storageNamespace string, branch string) error {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "storageNamespace", IsValid: ValidateStorageNamespace(storageNamespace)},
		{Name: "branch", IsValid: ValidateBranchName(branch)},
	}); err != nil {
		return err
	}

	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		// next id for branch
		var branchID int64
		if err := tx.Get(&branchID, `SELECT nextval('catalog_branches_id_seq')`); err != nil {
			return nil, fmt.Errorf("next next id: %w", err)
		}

		// next id for repository
		var repoID int64
		if err := tx.Get(&repoID, `SELECT nextval('catalog_repositories_id_seq')`); err != nil {
			return nil, fmt.Errorf("next repository id: %w", err)
		}
		if _, err := tx.Exec(`SET CONSTRAINTS catalog_repositories_branches_id_fk DEFERRED`); err != nil {
			return nil, fmt.Errorf("set constranits: %w", err)
		}

		// create repository with ref to branch
		if _, err := tx.Exec(`INSERT INTO catalog_repositories (id,name,storage_namespace,creation_date,default_branch)
			VALUES ($1,$2,$3,transaction_timestamp(),$4)`, repoID, repository, storageNamespace, branchID); err != nil {
			return nil, fmt.Errorf("insert repository: %w", err)
		}

		// create branch with ref to repository
		if _, err := tx.Exec(`INSERT INTO catalog_branches (repository_id,id,name)
			VALUES ($1,$2,$3)`, repoID, branchID, branch); err != nil {
			return nil, fmt.Errorf("insert branch: %w", err)
		}

		// create initial commit
		_, err := tx.Exec(`INSERT INTO catalog_commits (branch_id,commit_id,committer,message,creation_date,previous_commit_id)
			VALUES ($1,nextval('catalog_commit_id_seq'),$2,$3,transaction_timestamp(),0)`,
			branchID, CatalogerCommitter, createRepositoryCommitMessage)
		if err != nil {
			return nil, fmt.Errorf("insert commit: %w", err)
		}
		return repoID, nil
	}, c.txOpts(ctx)...)
	return err
}
