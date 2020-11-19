package mvcc

import (
	"context"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

const useEntryReadBatched = true

func (c *cataloger) GetEntry(ctx context.Context, repository, reference string, path string, params catalog.GetEntryParams) (*catalog.Entry, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "reference", IsValid: ValidateReference(reference)},
	}); err != nil {
		return nil, err
	}
	if path == "" {
		return nil, db.ErrNotFound
	}
	ref, err := catalog.ParseRef(reference)
	if err != nil {
		return nil, err
	}

	var entry *catalog.Entry
	if useEntryReadBatched {
		entry, err = c.getEntryBatchMaybeExpired(ctx, repository, *ref, path)
	} else {
		entry, err = c.getEntryMaybeExpired(ctx, repository, *ref, path)
	}
	if !params.ReturnExpired && entry != nil && entry.Expired {
		return entry, catalog.ErrExpired
	}
	return entry, err
}

func (c *cataloger) getEntryBatchMaybeExpired(ctx context.Context, repository string, ref catalog.Ref, path string) (*catalog.Entry, error) {
	replyChan := make(chan readResponse, 1) // used for a single return status message.
	// channel written to and closed by readEntriesBatch
	request := &readRequest{
		bufKey: bufferingKey{
			repository: repository,
			ref:        ref,
		},
		pathReq: pathRequest{
			path:      path,
			replyChan: replyChan,
		},
	}
	c.readEntryRequestChan <- request
	select {
	case response := <-replyChan:
		return response.entry, response.err
	case <-time.After(c.BatchRead.EntryMaxWait):
		return nil, ErrReadEntryTimeout
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *cataloger) getEntryMaybeExpired(ctx context.Context, repository string, ref catalog.Ref, path string) (*catalog.Entry, error) {
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

		var ent catalog.Entry
		if err := tx.Get(&ent, sql, args...); err != nil {
			return nil, err
		}
		return &ent, nil
	}, c.txOpts(ctx, db.ReadOnly())...)
	if err != nil {
		return nil, err
	}
	return res.(*catalog.Entry), nil
}
