package catalog

import (
	"context"

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
		if err := tx.Get(&branchID, `SELECT nextval('branches_id_seq')`); err != nil {
			return nil, err
		}

		// next id for repository
		var repoID int64
		if err := tx.Get(&repoID, `SELECT nextval('repositories_id_seq')`); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(`SET CONSTRAINTS repositories_branches_id_fkey DEFERRED`); err != nil {
			return nil, err
		}

		// create repository with ref to branch
		creationDate := c.clock.Now()
		if _, err := tx.Exec(`INSERT INTO repositories (id, name, storage_namespace, creation_date, default_branch)
			VALUES ($1,$2,$3,$4,$5)`, repoID, repository, storageNamespace, creationDate, branchID); err != nil {
			return nil, err
		}

		// create branch with ref to repository
		if _, err := tx.Exec(`INSERT INTO branches (repository_id, id, name)
			VALUES ($1,$2,$3)`, repoID, branchID, branch); err != nil {
			return nil, err
		}

		// create initial commit
		_, err := tx.Exec(`INSERT INTO commits (branch_id, commit_id, committer, message, creation_date)
			VALUES ($1,nextval('commit_id_seq'),$2,$3,$4)`,
			branchID, CatalogerCommitter, createRepositoryCommitMessage, creationDate)
		if err != nil {
			return nil, err
		}
		return repoID, nil
	}, c.txOpts(ctx)...)
	return err
}
