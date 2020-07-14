package catalog

import (
	"context"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

const ListEntriesByLevelMaxLimit = 1000

func (c *cataloger) ListEntriesByLevel(ctx context.Context, repository, reference, prefix, after, delimiter string, limit int) ([]LevelEntryResult, bool, error) {
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
		branchID, err := getBranchID(tx, repository, branchName, LockTypeNone)
		if err != nil {
			return nil, fmt.Errorf(" get branch ID failed: %w", err)
		}
		lineage, err := getLineage(tx, branchID, commitID)
		if err != nil {
			return nil, fmt.Errorf("get lineage failed: %w", err)
		}
		prefixQuery := sqListByPrefix(prefix, after, delimiter, branchID, limit+1, commitID, lineage)
		sql, args, err := prefixQuery.PlaceholderFormat(sq.Dollar).ToSql()
		if err != nil {
			return nil, fmt.Errorf("list by level ToSql failed : %w", err)
		}
		var markerList []LevelEntryResult
		err = tx.Select(&markerList, sql, args...)
		if err != nil {
			return nil, fmt.Errorf("list by level query failed : %w", err)
		}
		err = loadEntriesIntoMarkerList(markerList, tx, branchID, commitID, lineage, delimiter, prefix)
		if err != nil {
			return nil, err
		}
		return markerList, nil
	}, c.txOpts(ctx, db.ReadOnly())...)
	if err != nil {
		return nil, false, err
	}
	result := markers.([]LevelEntryResult)
	moreToRead := paginateSlice(&result, limit)
	return result, moreToRead, nil
}

func loadEntriesIntoMarkerList(markerList []LevelEntryResult, tx db.Tx, branchID int64, commitID CommitID, lineage []lineageCommit, delimiter, prefix string) error {
	type entryRun struct {
		startRunIndex, runLength   int
		startEntryRun, endEntryRun string
	}
	var entryRuns []entryRun
	var inRun bool
	var previousInRun string
	var run entryRun
	for i := range markerList {
		p := markerList[i].Path
		if strings.HasSuffix(p, DirectoryTermination) { // remove termination character, if present
			p = strings.TrimSuffix(p, DirectoryTermination)
			markerList[i].Path = p
		}
		if strings.HasSuffix(p, delimiter) { // terminating by '/'(slash) character is an indication of a directory
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
		rangeReader := sq.Select("path", "physical_address", "creation_date", "size", "checksum", "metadata").
			Where("path between ? and ?", prefix+r.startEntryRun, prefix+r.endEntryRun).FromSelect(entriesReader, "e")
		sql, args, err := rangeReader.PlaceholderFormat(sq.Dollar).ToSql()
		if err != nil {
			return fmt.Errorf("format entries select sql failed: %w", err)
		}
		var entriesList []Entry
		err = tx.Select(&entriesList, sql, args...)
		if err != nil {
			return fmt.Errorf("reading entries failed: %w", err)
		}
		if len(entriesList) != r.runLength {
			return fmt.Errorf("expect to read %d entries, got %d", r.runLength, len(entriesList))
		}
		for i := 0; i < r.runLength; i++ {
			markerList[r.startRunIndex+i].Entry = &entriesList[i]
		}
	}
	return nil
}
