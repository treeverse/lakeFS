package catalog

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/treeverse/lakefs/db"
)

const isBatched = true

var first_time = true

func (c *cataloger) GetEntryMaybeExpired(ctx context.Context, repository, reference string, path string) (*Entry, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "reference", IsValid: ValidateReference(reference)},
	}); err != nil {
		return nil, err
	}
	if path == "" {
		return nil, db.ErrNotFound
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
			Select("path", "physical_address", "creation_date", "size", "checksum", "metadata", "is_expired").
			FromSelect(sqEntriesLineage(branchID, ref.CommitID, lineage), "entries").
			Where(sq.Eq{"path": path, "is_deleted": false}).
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

func (c *cataloger) GetEntryMaybeExpiredBatched(ctx context.Context, repository, reference string, path string) (*Entry, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "reference", IsValid: ValidateReference(reference)},
	}); err != nil {
		return nil, err
	}
	if path == "" {
		return nil, db.ErrNotFound
	}
	ref, err := ParseRef(reference)
	if err != nil {
		return nil, err
	}
	entry, err := c.dbBatchEntryRead(repository, path, *ref)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (c *cataloger) GetEntry(ctx context.Context, repository, reference string, path string, params GetEntryParams) (*Entry, error) {
	var entry *Entry
	var err error

	if isBatched {
		entry, err = c.GetEntryMaybeExpiredBatched(ctx, repository, reference, path)
	} else {
		entry, err = c.GetEntryMaybeExpired(ctx, repository, reference, path)
	}

	if !params.ReturnExpired && entry != nil && entry.Expired {
		return entry, ErrExpired
	}
	return entry, err
}
