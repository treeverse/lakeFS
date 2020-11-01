package catalog

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

const DiffMaxLimit = 1000

type diffEffectiveCommits struct {
	// ParentEffectiveCommit last commit parent merged from child.
	// When no sync commit is found - set the commit ID to the point child's branch was created.
	ParentEffectiveCommit CommitID `db:"parent_effective_commit"`

	// ChildEffectiveCommit last commit child merged from parent.
	// If the child never synced with parent, the commit ID is set to 1.
	ChildEffectiveCommit CommitID `db:"child_effective_commit"`

	// ParentEffectiveLineage lineage at the ParentEffectiveCommit
	ParentEffectiveLineage []lineageCommit
}

func (c *cataloger) Diff(ctx context.Context, repository string, leftReference string, rightReference string, params DiffParams) (Differences, bool, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "leftReference", IsValid: ValidateReference(leftReference)},
		{Name: "rightReference", IsValid: ValidateReference(rightReference)},
	}); err != nil {
		return nil, false, err
	}

	// parse references
	leftRef, err := ParseRef(leftReference)
	if err != nil {
		return nil, false, fmt.Errorf("left reference: %w", err)
	}
	rightRef, err := ParseRef(rightReference)
	if err != nil {
		return nil, false, fmt.Errorf("right reference: %w", err)
	}

	if params.Limit < 0 || params.Limit > DiffMaxLimit {
		params.Limit = DiffMaxLimit
	}
	var diffParams doDiffParams
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		// get branch IDs
		leftBranchID, err := c.getBranchIDCache(tx, repository, leftRef.Branch)
		if err != nil {
			return nil, fmt.Errorf("left ref branch: %w", err)
		}
		rightBranchID, err := c.getBranchIDCache(tx, repository, rightRef.Branch)
		if err != nil {
			return nil, fmt.Errorf("right ref branch: %w", err)
		}

		diffParams = doDiffParams{
			Repository:    repository,
			LeftCommitID:  leftRef.CommitID,
			LeftBranchID:  leftBranchID,
			RightCommitID: rightRef.CommitID,
			RightBranchID: rightBranchID,
			DiffParams: DiffParams{
				// we request additional one (without returning it) for pagination (hasMore)
				Limit:            params.Limit + 1,
				After:            params.After,
				AdditionalFields: params.AdditionalFields,
			},
		}
		scanner, err := NewDiffScanner(tx, diffParams)
		if err != nil {
			return nil, err
		}
		differences := make(Differences, 0, params.Limit+1)
		for scanner.Next() {
			differences = append(differences, scanner.Value().Difference)
			if diffParams.Limit > -1 && len(differences) >= diffParams.Limit {
				break
			}
		}
		if scanner.Error() != nil {
			return nil, scanner.Error()
		}
		return differences, nil
	}, c.txOpts(ctx)...)
	if err != nil {
		c.log.WithError(err).
			WithFields(logging.Fields{
				"left_branch_id":  diffParams.LeftBranchID,
				"left_commit_id":  diffParams.LeftCommitID,
				"right_branch_id": diffParams.RightBranchID,
				"right_commit_id": diffParams.RightCommitID,
			}).Debug("Diff error")
		return nil, false, err
	}
	differences := res.(Differences)
	hasMore := paginateSlice(&differences, params.Limit)
	return differences, hasMore, nil
}
