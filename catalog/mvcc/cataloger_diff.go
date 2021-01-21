package mvcc

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

const DiffMaxLimit = 1000

func (c *cataloger) Diff(_ context.Context, _ string, _ string, _ string, _ catalog.DiffParams) (catalog.Differences, bool, error) {
	return nil, false, catalog.ErrFeatureNotSupported
}

func (c *cataloger) Compare(ctx context.Context, repository string, leftReference string, rightReference string, params catalog.DiffParams) (catalog.Differences, bool, error) {
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
			DiffParams: catalog.DiffParams{
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
		differences := make(catalog.Differences, 0, params.Limit+1)
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
	differences := res.(catalog.Differences)
	hasMore := paginateSlice(&differences, params.Limit)
	return differences, hasMore, nil
}
