package catalog

import (
	"context"

	sq "github.com/Masterminds/squirrel"

	"github.com/treeverse/lakefs/db"
)

// TODO(barak): add delimiter support - return entry and common prefix entries

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
		q := psql.
			Select("path", "physical_address", "creation_date", "size", "checksum", "metadata").
			FromSelect(sqEntriesLineage(branchID, ref.CommitID, lineage), "entries").
			Where(sq.And{sq.Like{"path": likePath}, sq.Eq{"is_deleted": false}, sq.Gt{"path": after}}).
			OrderBy("path")
		if limit >= 0 {
			q.Limit(uint64(limit) + 1)
		}

		sql, args, err := q.ToSql()
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
