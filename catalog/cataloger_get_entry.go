package catalog

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) GetEntry(ctx context.Context, repository, reference string, path string) (*Entry, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "reference", IsValid: ValidateReference(reference)},
		{Name: "path", IsValid: ValidatePath(path)},
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

		lineage, err := getLineage(tx, branchID, ref.CommitID)
		if err != nil {
			return nil, fmt.Errorf("get lineage: %w", err)
		}

		sql, args, err := psql.
			Select("path", "physical_address", "creation_date", "size", "checksum", "metadata").
			FromSelect(sqEntriesLineage(branchID, ref.CommitID, lineage), "entries").
			Where(sq.And{sq.Eq{"path": path}, sq.Eq{"is_deleted": false}}).
			ToSql()
		if err != nil {
			return nil, fmt.Errorf("build sql: %w", err)
		}

		var ent Entry
		if err := tx.Get(&ent, sql, args...); err != nil {
			return nil, err
		}
		return &ent, nil
	}, c.txOpts(ctx, db.ReadOnly())...)
	if err != nil {
		return nil, err
	}
	return res.(*Entry), nil
}
