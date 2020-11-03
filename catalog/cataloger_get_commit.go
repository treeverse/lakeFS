package catalog

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) GetCommit(ctx context.Context, repository, reference string) (*CommitLog, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "reference", IsValid: ValidateReference(reference)},
	}); err != nil {
		return nil, err
	}
	ref, err := ParseRef(reference)
	if err != nil {
		return nil, err
	}
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, ref.Branch)
		if err != nil {
			return nil, err
		}

		// for branch (committed or uncommitted) we reference last commit
		if ref.CommitID == UncommittedID || ref.CommitID == CommittedID {
			lastCommitID, err := getLastCommitIDByBranchID(tx, branchID)
			if err != nil {
				return nil, fmt.Errorf("get last commit id: %w", err)
			}
			ref.CommitID = lastCommitID
		}

		// read commit log information
		query := `SELECT b.name as branch_name,c.commit_id,c.previous_commit_id,c.committer,c.message,c.creation_date,c.metadata,
			COALESCE(bb.name,'') as merge_source_branch_name,COALESCE(c.merge_source_commit,0) as merge_source_commit
			FROM catalog_commits c JOIN catalog_branches b ON b.id = c.branch_id 
				LEFT JOIN catalog_branches bb ON bb.id = c.merge_source_branch
			WHERE b.id=$1 AND c.commit_id=$2`
		var rawCommit commitLogRaw
		if err := tx.Get(&rawCommit, query, branchID, ref.CommitID); err != nil {
			return nil, err
		}
		commit := convertRawCommit(rawCommit)
		return commit, nil
	}, c.txOpts(ctx, db.ReadOnly())...)
	if err != nil {
		return nil, err
	}
	return res.(*CommitLog), err
}

func convertRawCommit(raw commitLogRaw) *CommitLog {
	metadata := raw.Metadata
	if metadata == nil {
		metadata = make(Metadata)
	}
	c := &CommitLog{
		Reference:    MakeReference(raw.BranchName, raw.CommitID),
		Committer:    raw.Committer,
		Message:      raw.Message,
		CreationDate: raw.CreationDate,
		Metadata:     metadata,
	}
	if raw.MergeSourceBranchName != "" && raw.MergeSourceCommit > 0 {
		reference := MakeReference(raw.MergeSourceBranchName, raw.MergeSourceCommit)
		c.Parents = append(c.Parents, reference)
	}
	if raw.PreviousCommitID > 0 {
		reference := MakeReference(raw.BranchName, raw.PreviousCommitID)
		c.Parents = append(c.Parents, reference)
	}
	return c
}
