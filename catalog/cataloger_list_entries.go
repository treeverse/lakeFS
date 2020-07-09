package catalog

import (
	"context"

	sq "github.com/Masterminds/squirrel"

	"github.com/treeverse/lakefs/db"
)

const ListEntriesMaxLimit = 10000

func (c *cataloger) ListEntries(ctx context.Context, repository, reference string, prefix, after string, limit int) ([]*Entry, bool, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "reference", IsValid: ValidateReference(reference)},
	}); err != nil {
		return nil, false, err
	}

	ref, err := ParseRef(reference)
	if err != nil {
		return nil, false, err
	}

	if limit < 0 {
		limit = ListEntriesMaxLimit
	}
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repository, ref.Branch, LockTypeNone)
		if err != nil {
			return nil, err
		}

		likePath := db.Prefix(prefix)

		lineage, err := getLineage(tx, branchID, ref.CommitID)
		if err != nil {
			return nil, err
		}
		sql, args, err := psql.
			Select("path", "physical_address", "creation_date", "size", "checksum", "metadata").
			FromSelect(sqEntriesLineage(branchID, ref.CommitID, lineage), "entries").
			Where(sq.And{sq.Like{"path": likePath}, sq.Eq{"is_deleted": false}, sq.Gt{"path": after}}).
			OrderBy("path").
			Limit(uint64(limit) + 1).
			ToSql()
		if err != nil {
			return nil, err
		}
		var entries []*Entry
		if err := tx.Select(&entries, sql, args...); err != nil {
			return nil, err
		}
		return entries, nil
	}, c.txOpts(ctx, db.ReadOnly())...)

	if err != nil {
		return nil, false, err
	}
	entries := res.([]*Entry)
	hasMore := paginateSlice(&entries, limit)
	return entries, hasMore, err
}
