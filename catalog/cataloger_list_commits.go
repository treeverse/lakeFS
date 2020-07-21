package catalog

import (
	"context"
	"math"

	"github.com/treeverse/lakefs/db"
)

const ListCommitsMaxLimit = 10000

func (c *cataloger) ListCommits(ctx context.Context, repository, branch string, fromReference string, limit int) ([]*CommitLog, bool, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "branch", IsValid: ValidateBranchName(branch)},
		{Name: "fromReference", IsValid: ValidateOptionalString(fromReference, IsValidReference)},
	}); err != nil {
		return nil, false, err
	}
	ref, err := ParseRef(fromReference)
	if err != nil {
		return nil, false, err
	}
	if limit < 0 || limit > ListCommitsMaxLimit {
		limit = ListCommitsMaxLimit
	}
	// we start from the newest to the oldest
	fromCommitID := CommitID(math.MaxInt64)
	if ref.CommitID > 0 {
		fromCommitID = ref.CommitID
	}
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, branch)
		if err != nil {
			return nil, err
		}
		query := `SELECT c.commit_id, c.committer, c.message, c.creation_date, c.metadata
			FROM commits c JOIN branches b ON b.id = c.branch_id 
			WHERE b.id = $1 AND c.commit_id < $2
			ORDER BY c.commit_id DESC
			LIMIT $3`
		var rawCommits []*commitLogRaw
		if err := tx.Select(&rawCommits, query, branchID, fromCommitID, limit+1); err != nil {
			return nil, err
		}
		commits := convertRawCommits(branch, rawCommits)
		return commits, nil
	}, c.txOpts(ctx, db.ReadOnly())...)

	if err != nil {
		return nil, false, err
	}
	commits := res.([]*CommitLog)
	hasMore := paginateSlice(&commits, limit)
	return commits, hasMore, err
}

func convertRawCommits(branch string, rawCommits []*commitLogRaw) []*CommitLog {
	commits := make([]*CommitLog, len(rawCommits))
	for i, commit := range rawCommits {
		commits[i] = convertRawCommit(branch, commit)
	}
	return commits
}
