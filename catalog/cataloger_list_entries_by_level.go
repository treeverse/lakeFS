package catalog

import (
	"context"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

const ListEntriesByLevelMaxLimit = 1000

func (c *cataloger) ListEntriesByLevel(ctx context.Context, repository, reference, prefix, after, delimiter string, limit int) ([]LevelEntry, bool, error) {
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
	if limit < 0 || limit > ListEntriesByLevelMaxLimit {
		limit = ListEntriesByLevelMaxLimit
	}
	branchName := ref.Branch
	commitID := ref.CommitID
	markers, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
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
	if err != nil {
		return nil, false, err
	}
	result := markers.([]LevelEntry)
	moreToRead := paginateSlice(&result, limit)
	return result, moreToRead, nil
}

func loadEntriesIntoMarkerList(markerList []string, tx db.Tx, branchID int64, commitID CommitID, lineage []lineageCommit, delimiter, prefix string) ([]LevelEntry, error) {
	type entryRun struct {
		startRunIndex, runLength   int
		startEntryRun, endEntryRun string
	}
	var entryRuns []entryRun
	var inRun bool
	var previousInRun string
	var run entryRun
	entries := make([]LevelEntry, len(markerList))
	for i, p := range markerList {
		// remove termination character, if present
		p = strings.TrimSuffix(p, DirectoryTermination)
		entries[i].Path = prefix + p
		if strings.HasSuffix(p, delimiter) { // terminating by '/'(slash) character is an indication of a directory
			entries[i].CommonLevel = true
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
		var entriesList []Entry
		err = tx.Select(&entriesList, sql, args...)
		if err != nil {
			return nil, fmt.Errorf("select entries: %w", err)
		}
		if len(entriesList) != r.runLength {
			return nil, fmt.Errorf("expect to read %d entries, got %d", r.runLength, len(entriesList))
		}
		for i := 0; i < r.runLength; i++ {
			entries[r.startRunIndex+i].Entry = entriesList[i]
		}
	}
	return entries, nil
}
