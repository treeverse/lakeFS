package catalog

import (
	"context"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"

	"github.com/treeverse/lakefs/db"
)

const ListEntriesMaxLimit = 10000

func (c *cataloger) ListEntries(ctx context.Context, repository, reference string, prefix, after string, delimiter string, limit int) ([]*Entry, bool, error) {
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

	if limit < 0 || limit > ListEntriesMaxLimit {
		limit = ListEntriesMaxLimit
	}

	var res interface{}
	switch delimiter {
	case "":
		res, err = c.listEntries(ctx, repository, ref, prefix, after, limit)
	case DefaultPathDelimiter:
		res, err = c.listEntriesByLevel(ctx, repository, ref, prefix, after, delimiter, limit)
	default:
		err = ErrUnsupportedDelimiter
	}
	if err != nil {
		return nil, false, err
	}
	result := res.([]*Entry)
	moreToRead := paginateSlice(&result, limit)
	return result, moreToRead, nil
}

func (c *cataloger) listEntries(ctx context.Context, repository string, ref *Ref, prefix string, after string, limit int) (interface{}, error) {
	return c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, ref.Branch)
		if err != nil {
			return nil, err
		}

		likePath := db.Prefix(prefix)
		lineage, err := getLineage(tx, branchID, ref.CommitID)
		if err != nil {
			return nil, fmt.Errorf("get lineage: %w", err)
		}
		sql, args, err := psql.
			Select("path", "physical_address", "creation_date", "size", "checksum", "metadata").
			FromSelect(sqEntriesLineage(branchID, ref.CommitID, lineage), "entries").
			// Listing also shows expired objects!
			Where(sq.And{sq.Like{"path": likePath}, sq.Eq{"is_deleted": false}, sq.Gt{"path": after}}).
			OrderBy("path").
			Limit(uint64(limit) + 1).
			ToSql()
		if err != nil {
			return nil, fmt.Errorf("build sql: %w", err)
		}
		var entries []*Entry
		if err := tx.Select(&entries, sql, args...); err != nil {
			return nil, err
		}
		return entries, nil
	}, c.txOpts(ctx, db.ReadOnly())...)
}

func (c *cataloger) listEntriesByLevel(ctx context.Context, repository string, ref *Ref, prefix string, after string, delimiter string, limit int) (interface{}, error) {
	branchName := ref.Branch
	commitID := ref.CommitID
	return c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, branchName)
		if err != nil {
			return nil, err
		}
		lineage, err := getLineage(tx, branchID, commitID)
		if err != nil {
			return nil, fmt.Errorf("get lineage: %w", err)
		}

		listAfter := strings.TrimPrefix(after, prefix)
		prefixQuery := sqListByPrefix(prefix, listAfter, delimiter, branchID, limit+1, commitID, lineage)
		sql, args, err := prefixQuery.PlaceholderFormat(sq.Dollar).ToSql()
		if err != nil {
			return nil, fmt.Errorf("build sql: %w", err)
		}
		var markerList []string
		err = tx.Select(&markerList, sql, args...)
		if err != nil {
			return nil, fmt.Errorf("select: %w", err)
		}
		return loadEntriesIntoMarkerList(markerList, tx, branchID, commitID, lineage, delimiter, prefix)
	}, c.txOpts(ctx, db.ReadOnly())...)
}

func loadEntriesIntoMarkerList(markerList []string, tx db.Tx, branchID int64, commitID CommitID, lineage []lineageCommit, delimiter, prefix string) ([]*Entry, error) {
	type entryRun struct {
		startRunIndex, runLength   int
		startEntryRun, endEntryRun string
	}
	var entryRuns []entryRun
	var inRun bool
	var previousInRun string
	var run entryRun
	for i, p := range markerList {
		// remove termination character, if present
		p = strings.TrimSuffix(p, DirectoryTermination)
		markerList[i] = p // update the marker list - used later for common level
		// terminating by '/' (delimiter) character is an indication of a directory
		if strings.HasSuffix(p, delimiter) {
			// its absence indicates a leaf entry that has to be read from DB
			if inRun {
				inRun = false
				run.endEntryRun = previousInRun
				entryRuns = append(entryRuns, run)
			}
		} else { // an entry
			previousInRun = p
			if !inRun {
				inRun = true
				run.startEntryRun = p
				run.runLength = 1
				run.startRunIndex = i
			} else {
				run.runLength++
			}
		}
	}
	if inRun {
		run.endEntryRun = previousInRun
		entryRuns = append(entryRuns, run)
	}

	entries := make([]*Entry, len(markerList))
	// fill entries by going over entryRuns
	entriesReader := sqEntriesLineageV(branchID, commitID, lineage)
	for _, r := range entryRuns {
		sql, args, err := sq.Select("path", "physical_address", "creation_date", "size", "checksum", "metadata").
			Where("NOT is_deleted AND path BETWEEN ? and ?", prefix+r.startEntryRun, prefix+r.endEntryRun).
			FromSelect(entriesReader, "e").
			PlaceholderFormat(sq.Dollar).
			ToSql()
		if err != nil {
			return nil, fmt.Errorf("build entries sql: %w", err)
		}
		var entriesList []*Entry
		err = tx.Select(&entriesList, sql, args...)
		if err != nil {
			return nil, fmt.Errorf("select entries: %w", err)
		}
		if len(entriesList) != r.runLength {
			return nil, fmt.Errorf("expect to read %d entries, got %d", r.runLength, len(entriesList))
		}
		for i := 0; i < r.runLength; i++ {
			entries[r.startRunIndex+i] = entriesList[i]
		}
	}
	// fill common prefix by filling the missing parts based on our markers
	for i, p := range markerList {
		if entries[i] == nil {
			entries[i] = &Entry{
				CommonLevel: true,
				Path:        prefix + p,
			}
		}
	}
	return entries, nil
}
