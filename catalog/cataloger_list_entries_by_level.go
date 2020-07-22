package catalog

import (
	"context"
	"errors"
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
		markerList, err := loopByLevel(tx, prefix, after, delimiter, limit, branchID, commitID, lineage)
		return loadEntriesIntoMarkerList(markerList, tx, branchID, commitID, lineage, delimiter, prefix)
	}, c.txOpts(ctx, db.ReadOnly())...)
	if err != nil {
		return nil, false, err
	}
	result := markers.([]LevelEntry)
	moreToRead := paginateSlice(&result, limit)
	return result, moreToRead, nil
}

func loopByLevel(tx db.Tx, prefix, after, delimiter string, limit int, branchID int64, requestedCommit CommitID, lineage []lineageCommit) ([]string, error) {
	var err error
	limit += 1
	unionSQL := buildLevelQuery(branchID, lineage, 4)
	endOfPrefixRange := prefix + DirectoryTermination
	nextPath := make([]string, len(lineage)+1)
	listAfter := prefix + strings.TrimPrefix(after, prefix)
	var markerList []string
	for i := 0; i < limit; i++ {

		err = tx.Get(&nextPath, unionSQL, listAfter, endOfPrefixRange)
		if errors.As(err, &db.ErrNotFound) {
			return markerList, nil
		}
		if err != nil {
			return nil, err
		}
		nextPart := nextPath[len(prefix):]
		pos := strings.Index(nextPart, delimiter)
		if pos != -1 {
			nextPath = nextPath[:len(prefix)+pos+1]
			listAfter = nextPath + DirectoryTermination
		} else {
			listAfter = nextPath
		}
		markerList = append(markerList, nextPath)
	}
	return markerList, nil
}

type responseRow struct {
	branchID  int64
	path      string
	minCommit CommitID
	maxCommit CommitID
}
type responseRows []responseRow

func doOneStrech(tx db.Tx, prefix string, delimiter string, branchID int64, limit int, numOfBranches int) (string, error) {
	var resultBuf []responseRow
	var nextPath string
	pathCandidates := make([]string, numOfBranches)
	readPrefixes := make([]string, numOfBranches)
	for i := 0; i < numOfBranches; i++ {
		readPrefixes[i] = prefix
	}

	foundPath := false
	for !foundPath {

	}

}
func buildLevelQuery(baseBranchID int64, lineage []lineageCommit, limit int, requestedCommit CommitID) []sq.SelectBuilder {
	rowSelect := sq.Select("branch_id as branchID", "path", "min_commit as minCommit", "max_commit as maxCommit").
		From("entries").
		OrderBy("branch_id", "path", "min_commit desc").
		Limit(uint64(limit))
	unionParts := make([]sq.SelectBuilder, len(lineage)+1)
	unionParts[0] = rowSelect.Where("branch_id = ?", baseBranchID).
		Where("min_commit <=  ? and (max_commit > ? or max_commit = 0", requestedCommit, requestedCommit)

	unionParts[0] = fmt.Sprintf(
		`select * from (select branch_id as branchID,path,min_commit as minCommit ,max_commit as maxCommit from entries 
						where branch_id = %d 
						and path >  $1 and path < $2 and (max_commit = max_commit_id() or max_commit = 0)
						order by branch_id,path,min_commit desc
						limit %d) t`, baseBranchID, limit)

	for _, l := range lineage {
		singleBranchQuery := fmt.Sprintf(
			`select * from (select branch_id,path,min_commit,max_commit from entries  
						where branch_id = %d  and min_commit between 1 and  %d and (max_commit >= %d  or max_commit = 0)
						and path >  $1 and path < $2
						order by branch_id,path,min_commit desc
						limit %d) t`, l.BranchID, l.CommitID, l.CommitID, limit)
		unionParts = append(unionParts, singleBranchQuery)
	}
	query := strings.Join(unionParts, "\n union all\n")
	return query
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
			Where("path between ? and ?", prefix+r.startEntryRun, prefix+r.endEntryRun).
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
