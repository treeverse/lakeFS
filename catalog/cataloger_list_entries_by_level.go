package catalog

import (
	"context"
	"fmt"
	"unicode/utf8"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

type listResultStruct struct {
	Path  *string `db:"path"`
	Entry *Entry
}

/*
unc (c *cataloger) ListEntries(ctx context.Context, repository, reference string, prefix, after string, limit int) ([]*Entry, bool, error) {
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

*/

func (c *cataloger) ListEntriesByLevel(ctx context.Context, repository, reference, prefix, after, delimiter string, limit int) ([]listResultStruct, bool, error) {
	var moreToRead bool
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
	branchName := ref.Branch
	commitID := ref.CommitID
	markers, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repository, branchName, LockTypeNone)
		if err != nil {
			return nil, fmt.Errorf("ListEntriesByLevel - get branch ID failed: %w", err)
		}
		lineage, err := getLineage(tx, branchID, commitID)
		if err != nil {
			return nil, fmt.Errorf(" ListEntriesByLevel -lineage failed: %w", err)
		}
		prefixQuery := sqListByPrefix(prefix, after, delimiter, branchID, limit, commitID, lineage)
		debugSQL := sq.DebugSqlizer(prefixQuery)
		_ = debugSQL
		sql, args, err := prefixQuery.PlaceholderFormat(sq.Dollar).ToSql()
		if err != nil {
			return nil, fmt.Errorf(" ListEntriesByLevel - dirlist ToSql failed : %w", err)
		}
		var markerList []listResultStruct
		err = tx.Select(&markerList, sql, args...)
		if err != nil {
			return nil, fmt.Errorf(" ListEntriesByLevel - dirList query failed : %w", err)
		}
		if len(markerList) > limit { // remove last entry that indicates there are more
			markerList = markerList[:limit]
			moreToRead = true
		}
		type entryRun struct {
			startRunIndex, runLength   int
			startEntryRun, endEntryRun *string
		}
		var entryRuns []entryRun
		var inRun bool
		var PreviousInRun *string
		var run *entryRun
		for i, _ := range markerList {
			p := markerList[i].Path
			if len(*p) == 0 {
				return nil, fmt.Errorf(" ListEntriesByLevel - an empty string returned as path : %w", err)
			}
			r, size := utf8.DecodeLastRuneInString(*p)
			if string(r) == DirectoryTeminationChar { // unicode character of value 1_000_000 is an indication of a directory
				// its absence indicates a leaf entry that has to be read from DB
				*p = (*p)[:len(*p)-size]
				if inRun {
					inRun = false
					run.endEntryRun = PreviousInRun
					entryRuns = append(entryRuns, *run)
					run = nil
				}
			} else { // an entry
				PreviousInRun = p
				if !inRun {
					inRun = true
					run = &entryRun{}
					run.startEntryRun = p
					run.runLength = 1
					run.startRunIndex = i
				} else {
					run.runLength++
				}
			}

		}
		if inRun {
			run.endEntryRun = PreviousInRun
			entryRuns = append(entryRuns, *run)
		}
		entriesReader := sqEntriesLineageV(branchID, commitID, lineage)
		for _, r := range entryRuns {
			entriesList := make([]Entry, 0)
			rangeReader := sq.Select("path", "physical_address", "creation_date", "size", "checksum", "metadata").
				Where("path between ? and ?", prefix+*r.startEntryRun, prefix+*r.endEntryRun).FromSelect(entriesReader, "e")
			sql, args, err := rangeReader.PlaceholderFormat(sq.Dollar).ToSql()
			if err != nil {
				return nil, fmt.Errorf(" ListEntriesByLevel - rangeReader ToSql failed : %w", err)
			}
			err = tx.Select(&entriesList, sql, args...)
			if err != nil {
				return nil, fmt.Errorf(" ListEntriesByLevel - reading entries failed : %w", err)
			}
			if len(entriesList) != r.runLength {
				errStr := fmt.Sprintf("ListEntriesByLevel - expecte to read %d entries, got %d", r.runLength, len(entriesList)) + " : %w"
				return nil, fmt.Errorf(errStr, err)
			}
			for i := 0; i < r.runLength; i++ {
				markerList[r.startRunIndex+i].Entry = &entriesList[i]
			}
		}
		return markerList, nil
	}, c.txOpts(ctx, db.ReadOnly())...)
	return markers.([]listResultStruct), moreToRead, err
}
