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

func loopByLevel(tx db.Tx, prefix, after, delimiter string, limit, batchSize int, branchID int64, requestedCommit CommitID, lineage []lineageCommit) ([]string, error) {
	lowestCommitID := CommitID(1)
	topCommitID := requestedCommit
	if requestedCommit == 0 {
		lowestCommitID = 0
	}
	if requestedCommit <= 0 {
		topCommitID = MaxCommitID
	}
	sortedBranches := make([]int64, len(lineage)+1)
	sortedBranches[0] = branchID
	for i, l := range lineage {
		sortedBranches[i+1] = l.BranchID
	}
	limit += 1
	unionQueryParts := buildBaseLevelQuery(branchID, lineage, batchSize, lowestCommitID, topCommitID)
	endOfPrefixRange := prefix + DirectoryTermination
	//nextPath := make([]string, len(lineage)+1)
	//listAfter := prefix + strings.TrimPrefix(after, prefix)
	listAfter := make([]string, len(lineage)+1)
	for i := 0; i < len(listAfter); i++ {
		listAfter[i] = prefix + strings.TrimPrefix(after, prefix)
	}
	var markerList []string
	pathCond := "path > ? and path < '" + endOfPrefixRange + "'"
	unionSelect := unionQueryParts[0].Where(pathCond, listAfter[0]).Prefix("(").Suffix(")")
	var responseRows []resultRow
	for i := 0; i < limit; i++ {
		for j := 1; j < len(lineage)+1; j++ {
			unionSelect = unionSelect.SuffixExpr(sq.ConcatExpr("\n UNION ALL \n", "(",
				unionQueryParts[j].Where(pathCond, listAfter[j]), ")"))
		}
		fullQuery := sq.Select("*").FromSelect(unionSelect, "u")
		deb := sq.DebugSqlizer(fullQuery)
		_ = deb
		unionSQL, args, err := fullQuery.PlaceholderFormat(sq.Dollar).ToSql()
		err = tx.Select(&responseRows, unionSQL, args...)
		if errors.As(err, &db.ErrNotFound) {
			return markerList, nil
		}
		if err != nil {
			return nil, err
		}
		trimmedResponseRows := make([]resultRow, len(responseRows))
		for i, r := range responseRows {
			trimmedResponseRows[i] = r
			trimmedResponseRows[i].Path = r.Path[len(prefix):]
		}
		findCommonPrefix(trimmedResponseRows, lineage, delimiter, sortedBranches)
		/*nextPart := nextPath[len(prefix):]
		pos := strings.Index(nextPart, delimiter)
		if pos != -1 {
			nextPath = nextPath[:len(prefix)+pos+1]
			listAfter = nextPath + DirectoryTermination
		} else {
			listAfter = nextPath
		}*/
		//markerList = append(markerList, nextPath)
	}
	return markerList, nil
}

// heap implementation, using "container/heap"
type responseRowHeapType []resultRow

func (h responseRowHeapType) Len() int { return len(h) }
func (h responseRowHeapType) Less(i, j int) bool {
	if (h[i]).Path == h[j].Path {
		return h[i].BranchID > h[j].BranchID
	} else {
		return h[i].Path < h[j].Path
	}
}
func (h responseRowHeapType) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *responseRowHeapType) Push(x interface{}) {
	*h = append(*h, x.(resultRow))
}
func (h *responseRowHeapType) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func findCommonPrefix(response []resultRow, lineage []lineageCommit, delimiter string, branches []int64) *string {
	rh := new(responseRowHeapType)
	heap.Init(rh)
	branchRanges := make(map[int64][]resultRow, len(lineage)+1)
	for len(response) > 0 {
		branch := response[0].BranchID
		i := 1
		for i < len(response) && response[i].BranchID == branch {
			i++
		}
		branchRanges[branch] = response[:i]
		response = response[i:]
	}
	// init heap
	for _, branch := range branches {
		heap.Push(rh, branchRanges[branch][0])
	}
	resRow := heap.Pop(rh).(resultRow)
	for true { // exit loop by break
		b := resRow.BranchID
		var i int
		numOfRes := len(branchRanges[b])
		// find range of rows with this name - only case it is not one is uncommitted tombstone
		for i < numOfRes {
			if branchRanges[b][i].Path == resRow.Path {
				i++
			} else {
				break
			}
		}
		if i == numOfRes {
			panic("got to end of list")
		}
		pathResults := branchRanges[b][:i] // take all results with this path
		if checkPath(pathResults) {        // minimal path was found
			return &resRow.Path
		}
		branchRanges[b] = branchRanges[b][i:] // remove  rows of this path
		heap.Push(rh, branchRanges[b][0])
		deletedRow := resRow
		resRow = heap.Pop(rh).(resultRow)
		for resRow.Path == deletedRow.Path {
			b = resRow.BranchID
			branchRanges[b] = branchRanges[b][1:]
			heap.Push(rh, branchRanges[b][0])
			resRow = heap.Pop(rh).(resultRow)
		}

	}

	//tombstones := make(map[string]int64)
	//for _, branch := range branches {
	//	branchResponse, found := branchRanges[branch]
	//	if !found {
	//		continue
	//	}
	//	for i, e := range branchResponse {
	//		_, found = tombstones[e.Path]
	//		if found {
	//			continue
	//		}
	//		if e.MaxCommit == MaxCommitID {
	//
	//		}
	//
	//	}
	//
	//}

	return nil
}

func checkPath(pathResults []resultRow) bool {
	if pathResults[0].MaxCommit == MaxCommitID { // top path not deleted
		for _, r := range pathResults[1:] {
			if r.MinCommit == 0 && r.MaxCommit == 0 { // uncommitted tombstone - has precedence
				return false
			}
			return true
		}
	} // top is deleted
	return false
}

//func doOneStrech(tx db.Tx, prefix string, delimiter string, BranchID int64, limit int, numOfBranches int) (string, error) {
//	var resultBuf []resultRow
//	var nextPath string
//	pathCandidates := make([]string, numOfBranches)
//	readPrefixes := make([]string, numOfBranches)
//	for i := 0; i < numOfBranches; i++ {
//		readPrefixes[i] = prefix
//	}
//
//	foundPath := false
//	for !foundPath {
//
//	}

//}
func buildBaseLevelQuery(baseBranchID int64, lineage []lineageCommit, brancEntryLimit int, lowestCommitId, topCommitID CommitID) []sq.SelectBuilder {
	rowSelect := sq.Select("branch_id as BranchID", "path", "min_commit as minCommit").
		From("entries").
		OrderBy("branch_id", "path", "min_commit desc").
		Limit(uint64(brancEntryLimit))
	unionParts := make([]sq.SelectBuilder, len(lineage)+1)
	unionParts[0] = rowSelect.Where("branch_id = ?", baseBranchID).
		Column("max_commit as maxCommit").
		Where("min_commit between ? and ?  and (max_commit >= ? or max_commit = 0)", lowestCommitId, topCommitID, topCommitID)

	for i, l := range lineage {
		unionParts[i+1] = rowSelect.
			Column("CASE WHEN max_commit >= ? THEN max_commit_id() ELSE max_commit END AS maxCommit").
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
