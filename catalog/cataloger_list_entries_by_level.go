package catalog

import (
	"container/heap"
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
		markerList, err := loopByLevel(tx, prefix, after, delimiter, limit, 8, branchID, commitID, lineage)
		return loadEntriesIntoMarkerList(markerList, tx, branchID, commitID, lineage, delimiter, prefix)
	}, c.txOpts(ctx, db.ReadOnly())...)
	if err != nil {
		return nil, false, err
	}
	result := markers.([]LevelEntry)
	moreToRead := paginateSlice(&result, limit)
	return result, moreToRead, nil
}

type resultRow struct {
	BranchID  int64
	Path      string
	MinCommit CommitID
	MaxCommit CommitID
}

func loopByLevel(tx db.Tx, prefix, after, delimiter string, limit, branchBatchSize int, branchID int64, requestedCommit CommitID, lineage []lineageCommit) ([]string, error) {
	// translate logical (umcommitted and commited )commit id to actual minCommit,maxCommit numbers in Rows
	lowestCommitID := CommitID(1)
	topCommitID := requestedCommit
	if requestedCommit == 0 {
		lowestCommitID = 0
	}
	if requestedCommit <= 0 {
		topCommitID = MaxCommitID
	}
	// list of branches ordered form son to ancestors
	sortedBranches := make([]int64, len(lineage)+1)
	sortedBranches[0] = branchID
	for i, l := range lineage {
		sortedBranches[i+1] = l.BranchID
	}
	limit += 1 // increase limit to get indication of more rows to come
	unionQueryParts := buildBaseLevelQuery(branchID, lineage, branchBatchSize, lowestCommitID, topCommitID, len(prefix))
	endOfPrefixRange := prefix + DirectoryTermination
	listAfter := prefix + strings.TrimPrefix(after, prefix)
	var markerList []string
	pathCond := "path > ? and path < '" + endOfPrefixRange + "'"
	var responseRows []resultRow
	for i := 0; i < limit; i++ {
		unionSelect := unionQueryParts[0].Where(pathCond, listAfter).Prefix("(").Suffix(")")
		for j := 1; j < len(lineage)+1; j++ {
			// add the path condition to each union part
			unionSelect = unionSelect.SuffixExpr(sq.ConcatExpr("\n UNION ALL \n", "(",
				unionQueryParts[j].Where(pathCond, listAfter), ")"))
		}
		fullQuery := sq.Select("*").FromSelect(unionSelect, "u")
		deb := sq.DebugSqlizer(fullQuery)
		_ = deb
		unionSQL, args, err := fullQuery.PlaceholderFormat(sq.Dollar).ToSql()
		if err != nil {
			return nil, err
		}
		responseRows = responseRows[:0]
		err = tx.Select(&responseRows, unionSQL, args...)
		if errors.As(err, &db.ErrNotFound) {
			return markerList, nil
		}
		if err != nil {
			return nil, err
		}
		pathSuffix := findCommonPrefix(responseRows, lineage, delimiter, sortedBranches)
		if pathSuffix == nil {
			return markerList, nil
		}
		markerList = append(markerList, *pathSuffix)
		if strings.HasSuffix(*pathSuffix, delimiter) {
			*pathSuffix += DirectoryTermination
		}
		listAfter = prefix + *pathSuffix
	}
	return markerList, nil
}

func findCommonPrefix(response []resultRow, lineage []lineageCommit, delimiter string, branches []int64) *string {
	// split results by branch
	branchRanges := make(map[int64][]resultRow, len(branches))
	for len(response) > 0 {
		b := response[0].BranchID
		i := 1
		for i < len(response) && response[i].BranchID == b {
			i++
		}
		branchRanges[b] = response[:i]
		response = response[i:]
	}
	// init heap
	rh := new(responseRowHeapType)
	heap.Init(rh)
	for _, b := range branches {
		heap.Push(rh, branchRanges[b][0]) // bug - not
	}
	lowestResult := heap.Pop(rh).(resultRow)
	for true { // exit loop by break
		p := lowestResult.Path
		b := lowestResult.BranchID
		var i int
		numOfRes := len(branchRanges[b])
		// find range of rows with this name - only cases it is not one are uncommitted or tombstone
		for i = 0; (i < numOfRes) && (branchRanges[b][i].Path) == p; i++ {
		}
		if i == numOfRes {
			panic("got to end of list - will be implemented later")
		}
		pathResults := branchRanges[b][:i] // take all results with this path
		if checkPath(pathResults) {        // minimal path was found
			pos := strings.Index(p, delimiter)
			if pos > -1 {
				p = p[:pos+1]
			}
			return &p
		}
		branchRanges[b] = branchRanges[b][i:] // remove  rows of this path
		heap.Push(rh, branchRanges[b][0])
		rejectedResult := lowestResult
		lowestResult = heap.Pop(rh).(resultRow)        // todo : deleted should not be filtered ???
		for lowestResult.Path == rejectedResult.Path { // clear results with the same path as the one that was rejected
			b = lowestResult.BranchID
			branchRanges[b] = branchRanges[b][1:] // to do - handle empty result
			heap.Push(rh, branchRanges[b][0])
			lowestResult = heap.Pop(rh).(resultRow)
		}
	}
	return nil
}

func checkPath(pathResults []resultRow) bool {
	if pathResults[0].MaxCommit != MaxCommitID { // top is deleted
		return false
	} // top path not deleted, but may have uncommitted tombstone
	for _, r := range pathResults[1:] {
		if r.MinCommit == 0 && r.MaxCommit == 0 { // uncommitted tombstone - has precedence
			return false
		}
	}
	return true
}

func buildBaseLevelQuery(baseBranchID int64, lineage []lineageCommit, brancEntryLimit int, lowestCommitId, topCommitID CommitID, prefixLen int) []sq.SelectBuilder {
	rowSelect := sq.Select("branch_id as BranchID", "min_commit as minCommit").
		Column("substr(path,?) as path", prefixLen+1).
		From("entries").
		OrderBy("branch_id", "path", "min_commit desc").
		Limit(uint64(brancEntryLimit))
	unionParts := make([]sq.SelectBuilder, len(lineage)+1)
	unionParts[0] = rowSelect.Where("branch_id = ?", baseBranchID).
		Column("max_commit as maxCommit").
		Where("min_commit between ? and ?  and (max_commit >= ? or max_commit = 0)", lowestCommitId, topCommitID, topCommitID)

	for i, l := range lineage {
		unionParts[i+1] = rowSelect.
			Column("CASE WHEN max_commit >= ? THEN max_commit_id() ELSE max_commit END AS maxCommit", l.CommitID).
			Where("branch_id = ?", l.BranchID).
			Where("min_commit between 1 and  ? and (max_commit > ? or max_commit = 0)", l.CommitID, l.CommitID)

	}
	return unionParts
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
