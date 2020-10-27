package catalog

import (
	"context"
	"fmt"

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
	fromCommitID := MaxCommitID
	if ref.CommitID > 0 {
		fromCommitID = ref.CommitID
	}
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, branch)
		if err != nil {
			return nil, err
		}
		lineage, err := getLineage(tx, branchID, fromCommitID)
		if err != nil {
			return nil, fmt.Errorf("get lineage: %w", err)
		}
		lineageAsValuesTable := getLineageAsValues(lineage, branchID, fromCommitID)
		cte := `WITH RECURSIVE lineage_graph AS (
    select branch_id,commit_id from ` + lineageAsValuesTable + `
	union all
	select * from (Select distinct on (c.branch_id,c.merge_source_branch) merge_source_branch,merge_source_commit from catalog_commits c
	join lineage_graph l on l.branch_id = c.branch_id and c.merge_type='from_child'  and c.merge_source_commit < l.commit_id
	order by c.branch_id,c.merge_source_branch,c.commit_id desc )t)
`
		query := cte + `SELECT b_name.name as branch_name,c.commit_id,c.previous_commit_id,c.committer,c.message,c.creation_date,c.metadata,
				COALESCE(bb.name,'') as merge_source_branch_name,COALESCE(c.merge_source_commit,0) as merge_source_commit
			FROM catalog_commits c JOIN lineage_graph l  ON  c.branch_id = l.branch_id and c.commit_id <= l.commit_id
				JOIN catalog_branches b_name ON c.branch_id = b_name.id
				LEFT JOIN catalog_branches bb ON bb.id = c.merge_source_branch
			WHERE c.commit_id < $1
			ORDER BY c.commit_id DESC
			LIMIT $2`

		var rawCommits []commitLogRaw
		if err := tx.Select(&rawCommits, query, fromCommitID, limit+1); err != nil {
			return nil, err
		}
		commits := convertRawCommits(rawCommits)
		return commits, nil
	}, c.txOpts(ctx, db.ReadOnly())...)

	if err != nil {
		return nil, false, err
	}
	commits := res.([]*CommitLog)
	hasMore := paginateSlice(&commits, limit)
	return commits, hasMore, err
}

func convertRawCommits(rawCommits []commitLogRaw) []*CommitLog {
	commits := make([]*CommitLog, len(rawCommits))
	for i, commit := range rawCommits {
		copy := commit
		commits[i] = convertRawCommit(&copy)
	}
	return commits
}
