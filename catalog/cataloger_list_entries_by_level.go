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
		markerList, err := loopByLevel(tx, prefix, after, delimiter, limit, 32, branchID, commitID, lineage)
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
	BranchID   int64    `db:"branch_id"`
	PathSuffix string   `db:"path_postfix"`
	MinCommit  CommitID `db:"min_commit"`
	MaxCommit  CommitID `db:"max_commit"`
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
	branchPriorityMap := make(map[int64]int, len(lineage)+1)
	branchPriorityMap[branchID] = 0
	for i, l := range lineage {
		branchPriorityMap[l.BranchID] = i + 1
	}
	limit += 1 // increase limit to get indication of more rows to come
	unionQueryParts := buildBaseLevelQuery(branchID, lineage, branchBatchSize, lowestCommitID, topCommitID, len(prefix))
	endOfPrefixRange := prefix + DirectoryTermination
	listAfter := prefix + strings.TrimPrefix(after, prefix)
	var markerList []string
	pathCond := "path > ? and path < '" + endOfPrefixRange + "'"
	var resultRows []resultRow
	for true { // exit by return
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
		resultRows = resultRows[:0]
		err = tx.Select(&resultRows, unionSQL, args...)
		if errors.As(err, &db.ErrNotFound) {
			return markerList, nil
		}
		if err != nil {
			return nil, err
		}
		if len(resultRows) == 0 {
			fmt.Print("FINISHED")
			return markerList, nil
		}
		pathSuffixs := findCommonPrefix(resultRows, lineage, delimiter, branchPriorityMap, limit-i)
		markerList = append(markerList, pathSuffixs...)
		if len(pathSuffixs) == 0 || len(markerList) >= limit {
			return markerList, nil
		}
		nextJump := pathSuffixs[len(pathSuffixs)-1]
		if strings.HasSuffix(nextJump, delimiter) {
			nextJump += DirectoryTermination
		}
		listAfter = prefix + nextJump
	}
	return markerList, nil
}

func findCommonPrefix(response []resultRow, lineage []lineageCommit, delimiter string, branchPriorityMap map[int64]int, limit int) []string {
	// split results by branch
	branchRanges := make(map[int64][]resultRow, len(branchPriorityMap))
	for len(response) > 0 {
		b := response[0].BranchID
		i := 1
		for i < len(response) && response[i].BranchID == b {
			i++
		}
		branchRanges[b] = response[:i]
		response = response[i:]
	}
	var resultPathes []string
	for true { // exit loop by return
		//find lowest result
		b := findLowestResultInBranches(branchRanges, branchPriorityMap)
		t := branchRanges[b]
		p := branchRanges[b][0].PathSuffix
		pathResults := getPathResultRows(p, &t)
		if checkPathNotDeleted(pathResults) { // minimal path was found
			pos := strings.Index(p, delimiter)
			if pos > -1 {
				p = p[:pos+1]
			}
			resultPathes = append(resultPathes, p)
			if pos > -1 || len(resultPathes) >= limit {
				return resultPathes
			}
		}
		// if path was rejected in a branch, it can not be viewed from branches "deeper" in the lineage chain.
		// the path is removed from the beginning of all results.
		// if the path is not at the start of any other result = nothing happens
		for _, results := range branchRanges {
			getPathResultRows(p, &results)
		}
	}
	return nil // will never be executed
}

func getPathResultRows(path string, branchResults *[]resultRow) []resultRow {
	i := 0
	resultLen := len(*branchResults)
	for (*branchResults)[i].PathSuffix == path {
		i++
		if i == resultLen {
			panic("need more rows ")
		}
	}
	returnSlice := (*branchResults)[:i]
	*branchResults = (*branchResults)[i:]
	return returnSlice
}

func findLowestResultInBranches(branchRanges map[int64][]resultRow, branchPriorityMap map[int64]int) int64 {
	firstTime := true
	var chosenBranch int64
	var chosenResults []resultRow
	for b, r := range branchRanges {
		if firstTime {
			chosenBranch = b
			chosenResults = r
			continue
		}
		if r[0].PathSuffix == chosenResults[0].PathSuffix {
			if branchPriorityMap[chosenBranch] > branchPriorityMap[b] {
				chosenBranch = b
				chosenResults = r
			}
		} else if r[0].PathSuffix < chosenResults[0].PathSuffix {
			chosenBranch = b
			chosenResults = r
		}
	}
	return chosenBranch
}

func checkPathNotDeleted(pathResults []resultRow) bool {
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
	rawSelect := sq.Select("branch_id", "min_commit").
		Column("substr(path,?) as path_postfix", prefixLen+1).
		From("entries").
		OrderBy("branch_id", "path", "min_commit desc").
		Limit(uint64(brancEntryLimit))
	unionParts := make([]sq.SelectBuilder, len(lineage)+1)
	unionParts[0] = rawSelect.Where("branch_id = ?", baseBranchID).
		Column("max_commit").
		Where("min_commit between ? and ? ", lowestCommitId, topCommitID)

	for i, l := range lineage {
		unionParts[i+1] = rawSelect.
			Column("CASE WHEN max_commit >= ? THEN max_commit_id() ELSE max_commit END AS max_commit", l.CommitID).
			Where("branch_id = ?", l.BranchID).
			Where("min_commit between 1 and  ? ", l.CommitID)

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
