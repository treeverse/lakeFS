package mvcc

import (
	"context"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

const (
	createBranchCommitMessageFormat = "Branch '%s' created, source '%s'"
)

func (c *cataloger) CreateBranch(ctx context.Context, repository, branch string, sourceRef string) (*catalog.CommitLog, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "branch", IsValid: ValidateBranchName(branch)},
		{Name: "sourceRef", IsValid: ValidateReference(sourceRef)},
	}); err != nil {
		return nil, err
	}

	ref, err := ParseRef(sourceRef)
	if err != nil {
		return nil, fmt.Errorf("source ref: %w", err)
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		_, err := tx.Exec("LOCK TABLE catalog_branches IN SHARE UPDATE EXCLUSIVE MODE")
		if err != nil {
			return nil, fmt.Errorf("lock branches for update: %w", err)
		}

		repoID, err := c.getRepositoryIDCache(tx, repository)
		if err != nil {
			return nil, err
		}

		// get source branch id
		var sourceBranchID int64
		if err := tx.GetPrimitive(&sourceBranchID, `SELECT id FROM catalog_branches WHERE repository_id=$1 AND name=$2`,
			repoID, ref.Branch); err != nil {
			return nil, fmt.Errorf("source branch id: %w", err)
		}

		// get source branch commit id or latest commit id, in case we reference a branch
		var sourceCommitID CommitID
		if ref.CommitID > UncommittedID {
			err = tx.GetPrimitive(&sourceCommitID, `SELECT commit_id FROM catalog_commits WHERE branch_id=$1 AND commit_id=$2`,
				sourceBranchID, ref.CommitID)
		} else {
			err = tx.GetPrimitive(&sourceCommitID, `SELECT MAX(commit_id) FROM catalog_commits WHERE branch_id=$1`,
				sourceBranchID)
		}
		if err != nil {
			return nil, fmt.Errorf("get source branch commit id: %w", err)
		}

		// next id for branch
		var branchID int64
		if err := tx.GetPrimitive(&branchID, `SELECT nextval('catalog_branches_id_seq')`); err != nil {
			return nil, fmt.Errorf("next branch id: %w", err)
		}

		// insert new branch
		if _, err := tx.Exec(`INSERT INTO catalog_branches (repository_id, id, name, lineage)
			VALUES($1,$2,$3,(SELECT $4::bigint||lineage FROM catalog_branches WHERE id=$4))`,
			repoID, branchID, branch, sourceBranchID); err != nil {
			return nil, fmt.Errorf("insert branch: %w", err)
		}

		insertReturns := struct {
			CommitID             CommitID  `db:"commit_id"`
			MergeSourceCommit    CommitID  `db:"merge_source_commit"`
			TransactionTimestamp time.Time `db:"transaction_timestamp"`
		}{}
		commitMsg := fmt.Sprintf(createBranchCommitMessageFormat, branch, sourceRef)
		err = tx.Get(&insertReturns, `INSERT INTO catalog_commits (branch_id,commit_id,previous_commit_id,committer,message,
			creation_date,merge_source_branch,merge_type,lineage_commits,merge_source_commit)
			VALUES ($1,nextval('catalog_commit_id_seq'),0,$2,$3,transaction_timestamp(),$4,'from_parent',
				($5::bigint || (select distinct on (branch_id) lineage_commits from catalog_commits
					where branch_id=$4 and merge_type='from_parent' and commit_id <= $5 order by branch_id,commit_id desc)),
					$5)
			RETURNING commit_id,merge_source_commit,transaction_timestamp()`,
			branchID, catalog.DefaultCommitter, commitMsg, sourceBranchID, sourceCommitID)
		if err != nil {
			return nil, fmt.Errorf("insert commit: %w", err)
		}
		reference := MakeReference(branch, insertReturns.CommitID)
		parentReference := MakeReference(ref.Branch, insertReturns.MergeSourceCommit)

		commitLog := &catalog.CommitLog{
			Committer:    catalog.DefaultCommitter,
			Message:      commitMsg,
			CreationDate: insertReturns.TransactionTimestamp,
			Reference:    reference,
			Parents:      []string{parentReference},
		}
		return commitLog, nil
	}, c.txOpts(ctx)...)
	if err != nil {
		return nil, err
	}
	commitLog := res.(*catalog.CommitLog)
	return commitLog, nil
}
