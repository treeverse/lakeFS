package catalog

import (
	"context"

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
		branchID, err := getBranchID(tx, repository, ref.Branch, LockTypeNone)
		if err != nil {
			return nil, err
		}
		query := `SELECT c.commit_id, c.committer, c.message, c.creation_date, c.metadata
			FROM commits c JOIN branches b ON b.id = c.branch_id 
			WHERE b.id = $1 AND c.commit_id = $2`
		args := []interface{}{branchID, ref.CommitID}
		var rawCommit commitLogRaw
		if err := tx.Get(&rawCommit, query, args...); err != nil {
			return nil, err
		}
		commit := convertRawCommit(ref.Branch, &rawCommit)
		return commit, nil
	}, c.txOpts(ctx, db.ReadOnly())...)

	if err != nil {
		return nil, err
	}
	return res.(*CommitLog), err
}

func convertRawCommit(branch string, raw *commitLogRaw) *CommitLog {
	return &CommitLog{
		Reference:    MakeReference(branch, raw.CommitID),
		Committer:    raw.Committer,
		Message:      raw.Message,
		CreationDate: raw.CreationDate,
		Metadata:     raw.Metadata,
	}
}
