package mvcc

import (
	"context"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

const (
	createRepositoryCommitMessage       = "Repository created"
	createRepositoryImportCommitMessage = "Repository import branch created"
)

func (c *cataloger) CreateRepository(ctx context.Context, repository string, storageNamespace string, branch string) (*catalog.Repository, error) {
	if err := catalog.Validate(catalog.ValidateFields{
		{Name: "repository", IsValid: catalog.ValidateRepositoryName(repository)},
		{Name: "storageNamespace", IsValid: catalog.ValidateStorageNamespace(storageNamespace)},
		{Name: "branch", IsValid: catalog.ValidateBranchName(branch)},
	}); err != nil {
		return nil, err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		// next id for import branch
		var importBranchID int64
		if err := tx.GetPrimitive(&importBranchID, `SELECT nextval('catalog_branches_id_seq')`); err != nil {
			return nil, fmt.Errorf("next id import branch: %w", err)
		}

		// next id for branch
		var branchID int64
		if err := tx.GetPrimitive(&branchID, `SELECT nextval('catalog_branches_id_seq')`); err != nil {
			return nil, fmt.Errorf("next id default branch: %w", err)
		}

		// next id for repository
		var repoID int64
		if err := tx.GetPrimitive(&repoID, `SELECT nextval('catalog_repositories_id_seq')`); err != nil {
			return nil, fmt.Errorf("next repository id: %w", err)
		}
		if _, err := tx.Exec(`SET CONSTRAINTS catalog_repositories_branches_id_fk DEFERRED`); err != nil {
			return nil, fmt.Errorf("set constranits: %w", err)
		}

		// create repository with ref to branch
		if _, err := tx.Exec(`INSERT INTO catalog_repositories (id,name,storage_namespace,creation_date,default_branch)
			VALUES ($1,$2,$3,transaction_timestamp(),$4)`, repoID, repository, storageNamespace, branchID); db.IsUniqueViolation(err) {
			return nil, fmt.Errorf("%s %w", repository, db.ErrAlreadyExists)
		} else if err != nil {
			return nil, fmt.Errorf("insert repository: %w", err)
		}

		// create import branch with ref to repository
		if _, err := tx.Exec(`INSERT INTO catalog_branches (repository_id,id,name)
			VALUES ($1,$2,$3)`, repoID, importBranchID, catalog.DefaultImportBranchName); err != nil {
			return nil, fmt.Errorf("insert import branch: %w", err)
		}

		// create initial commit for import branch
		var importCommitID catalog.CommitID
		err := tx.GetPrimitive(&importCommitID, `INSERT INTO catalog_commits (branch_id,commit_id,committer,message,creation_date,previous_commit_id)
			VALUES ($1,nextval('catalog_commit_id_seq'),$2,$3,transaction_timestamp(),0)
			RETURNING commit_id`,
			importBranchID, catalog.DefaultCommitter, createRepositoryImportCommitMessage)
		if err != nil {
			return nil, fmt.Errorf("insert commit: %w", err)
		}

		// create default branch as child of import branch
		if _, err := tx.Exec(`INSERT INTO catalog_branches (repository_id,id,name,lineage)
			VALUES($1,$2,$3,$4)`,
			repoID, branchID, branch, []int64{importBranchID}); err != nil {
			return nil, fmt.Errorf("insert default branch: %w", err)
		}

		// create initial commit on default branch - child of import branch
		var creationDate time.Time
		err = tx.GetPrimitive(&creationDate, `INSERT INTO catalog_commits (branch_id,commit_id,committer,message,creation_date,
				previous_commit_id,merge_source_branch,merge_type,lineage_commits,merge_source_commit)
			VALUES ($1,nextval('catalog_commit_id_seq'),$2,$3,transaction_timestamp(),0,$4,'from_parent',$5,$6)
			RETURNING creation_date`,
			branchID, catalog.DefaultCommitter, createRepositoryCommitMessage, importBranchID, []int64{int64(importCommitID)}, importCommitID)
		if err != nil {
			return nil, fmt.Errorf("insert commit: %w", err)
		}
		return &catalog.Repository{
			Name:             repository,
			StorageNamespace: storageNamespace,
			DefaultBranch:    branch,
			CreationDate:     creationDate,
		}, nil
	}, c.txOpts(ctx)...)
	if err != nil {
		return nil, err
	}
	return res.(*catalog.Repository), nil
}
