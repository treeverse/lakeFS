package mvcc

import (
	"context"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

const (
	ListEntriesMaxLimit        = 1000
	ListEntriesBranchBatchSize = 32
)

func (c *cataloger) ListEntries(ctx context.Context, repository, reference string, prefix, after string, delimiter string, limit int) ([]*catalog.Entry, bool, error) {
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
	case catalog.DefaultPathDelimiter:
		res, err = c.listEntriesByLevel(ctx, repository, ref, prefix, after, delimiter, limit)
	default:
		err = catalog.ErrUnsupportedDelimiter
	}
	if err != nil {
		return nil, false, err
	}
	result := res.([]*catalog.Entry)
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
		entriesSQL, args, err := psql.
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
		var entries []*catalog.Entry
		if err := tx.Select(&entries, entriesSQL, args...); err != nil {
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
		markerList, err := loopByLevel(tx, prefix, after, delimiter, limit, ListEntriesBranchBatchSize, branchID, commitID, lineage)
		if err != nil {
			return nil, err
		}
		return loadEntriesIntoMarkerList(markerList, tx, branchID, commitID, lineage, delimiter, prefix)
	}, c.txOpts(ctx, db.ReadOnly())...)
}

// reading is mainly done in the main loop at "loopByLevel". It may happen (hopefully rarely) in getMoreRows.
// variables needed for accessing the database are packed and passed down to getMoreRows
type readParamsType struct {
	tx              db.Tx
	prefix          string
	branchBatchSize int
	topCommitID     CommitID
	branchID        int64
	branchQueryMap  map[int64]sq.SelectBuilder
}

// Extracts the prefixes(directories)  that exist under a certain prefix. (drill down in the object store "directory tree"
// on each iteration, the function does the SQL retrieval from all lineage branches, and the calls "processSinglePrefix" to decide
// which prefix to return from all lineage branches.
// paths are parsed by the delimiter (usually '/')
// Searching for the next prefix is done by appending the largest possible utf-8 rune to the end  of the previous prefix. and querying
// all the branches of it's lineage. then looking for the lowest-value non-deleted entry path found across all  branches.
// for that a union query is issues. to skip possible deleted entries - we need to retrieve "branchBatchSize" of rows from each branch
func loopByLevel(tx db.Tx, prefix, after, delimiter string, limit, branchBatchSize int, branchID int64, requestedCommit CommitID, lineage []lineageCommit) ([]string, error) {

	topCommitID := requestedCommit
	if requestedCommit == UncommittedID {
		topCommitID = MaxCommitID
	} else if requestedCommit == CommittedID {
		topCommitID = MaxCommitID - 1 // do not take uncommitted min_commit
	}

	// list of branches ordered from child to ancestors
	branchPriorityMap := make(map[int64]int, len(lineage)+1)
	branchPriorityMap[branchID] = 0
	for i, l := range lineage {
		branchPriorityMap[l.BranchID] = i + 1
	}
	limit += 1 // increase limit to get indication of more rows to come
	branchQueryMap := buildBaseLevelQuery(branchID, lineage, branchBatchSize, topCommitID, len(prefix), prefix+DirectoryTermination)

	var exactFirst bool
	var listAfter string
	if len(after) == 0 {
		listAfter = prefix
		exactFirst = true
	} else {
		if strings.HasSuffix(after, delimiter) {
			after += DirectoryTermination
		}
		listAfter = after
		exactFirst = false
	}
	var markerList []string
	readParams := readParamsType{
		tx:              tx,
		prefix:          prefix,
		branchBatchSize: branchBatchSize,
		topCommitID:     topCommitID,
		branchID:        branchID,
		branchQueryMap:  branchQueryMap,
	}

	for {
		var pathCond string
		if exactFirst {
			exactFirst = false
			pathCond = ">="
		} else {
			pathCond = ">"
		}
		unionSelect := branchQueryMap[branchID].Where("path "+pathCond+" ? ", listAfter).Prefix("(").Suffix(")")
		for j := 0; j < len(lineage); j++ {
			// add the path condition to each union part
			b := lineage[j].BranchID
			unionSelect = unionSelect.SuffixExpr(sq.ConcatExpr("\n UNION ALL \n", "(",
				branchQueryMap[b].Where("path "+pathCond+" ?", listAfter), ")"))
		}
		fullQuery := sq.Select("*").FromSelect(unionSelect, "u")
		unionSQL, args, err := fullQuery.PlaceholderFormat(sq.Dollar).ToSql()
		if err != nil {
			return nil, err
		}
		resultRows := make([]entryPathPrefixInfo, 0, branchBatchSize*len(lineage)+1)
		err = tx.Select(&resultRows, unionSQL, args...)
		if err != nil {
			return nil, err
		}
		if len(resultRows) == 0 {
			return markerList, nil
		}
		pathSuffixes := processSinglePrefix(resultRows, delimiter, branchPriorityMap, limit-len(markerList), readParams)
		markerList = append(markerList, pathSuffixes...)
		if len(pathSuffixes) == 0 || len(markerList) >= limit {
			return markerList, nil
		}
		nextJump := pathSuffixes[len(pathSuffixes)-1]
		if strings.HasSuffix(nextJump, delimiter) {
			nextJump += DirectoryTermination
		}
		listAfter = prefix + nextJump
	}
}

// extract either a single prefix, or a list of leaf entries from  results of union-reading from lineage branches
// branch priority map reflect the lineage order, with lower numbers indicating higher priority. it is used to decide which
// branch returned the prefix when the same path was returned by more than one branch.
func processSinglePrefix(unionReadResults []entryPathPrefixInfo, delimiter string, branchPriorityMap map[int64]int, limit int, readParams readParamsType) []string {

	// split results by branch
	branchRanges := make(map[int64][]entryPathPrefixInfo, len(branchPriorityMap))
	for _, result := range unionReadResults {
		b := result.BranchID
		_, exists := branchRanges[b]
		if !exists {
			branchRanges[b] = make([]entryPathPrefixInfo, 0, readParams.branchBatchSize)
		}
		branchRanges[b] = append(branchRanges[b], result)
	}
	var resultPaths []string
	for { // exit loop by return
		b := findLowestResultInBranches(branchRanges, branchPriorityMap)
		entry := branchRanges[b][0]
		p := entry.PathSuffix
		if !entry.IsDeleted() { // minimal path was found
			pos := strings.Index(p, delimiter)
			if pos > -1 {
				p = p[:pos+1]
			}
			resultPaths = append(resultPaths, p)
			if pos > -1 || len(resultPaths) >= limit {
				return resultPaths
			}
		}
		// after path is processed, it can not be viewed from branches "deeper" in the lineage chain.
		// the path is removed from the beginning of all results.
		// if start of a result slice is bigger than path - nothing happens
		// can not be smaller as path was selected for being the smallest
		for branch, entries := range branchRanges {
			if entries[0].PathSuffix == p {
				if len(entries) > 1 {
					branchRanges[branch] = entries[1:]
				} else {
					delete(branchRanges, branch)
					_ = getMoreRows(p, branch, branchRanges, readParams)
				}
			}
		}
		if len(branchRanges) == 0 { // all branches exhausted, no more to read
			return resultPaths
		}
	}
}

//  "processSinglePrefix" calls this function when it exhausts a branch before finding the next path.
// getMoreRows reads more rows for that branch and stores the results directly into branchRanges.
func getMoreRows(path string, branch int64, branchRanges map[int64][]entryPathPrefixInfo, readParams readParamsType) error {
	readBuf := make([]entryPathPrefixInfo, 0, readParams.branchBatchSize)
	singleSelect := readParams.branchQueryMap[branch]
	requestedPath := readParams.prefix + path
	singleSelect = singleSelect.Where("path > ?", requestedPath)
	s, args, err := singleSelect.PlaceholderFormat(sq.Dollar).ToSql()
	if err != nil {
		return err
	}
	err = readParams.tx.Select(&readBuf, s, args...)
	if len(readBuf) == 0 {
		err = pgx.ErrNoRows
	}
	if err != nil {
		return err
	}
	branchRanges[branch] = readBuf
	return nil
}

// accepts query results for all branches in lineage , and examines the first entry of each branch result, looking
// for the lowest path. If more than one branch contains that path, it will select the entry from the higher-priority
// branch. (lowest number in branchPriority map)
func findLowestResultInBranches(branchRanges map[int64][]entryPathPrefixInfo, branchPriorityMap map[int64]int) int64 {
	firstTime := true
	var chosenBranch int64
	var chosenPath string
	for b, r := range branchRanges {
		if firstTime {
			chosenBranch = b
			chosenPath = r[0].PathSuffix
			firstTime = false
			continue
		}
		if r[0].PathSuffix == chosenPath {
			if branchPriorityMap[chosenBranch] > branchPriorityMap[b] {
				chosenBranch = b
			}
		} else if r[0].PathSuffix < chosenPath {
			chosenBranch = b
			chosenPath = r[0].PathSuffix
		}
	}
	return chosenBranch
}

// builds a map of select queries for each of the branches in  the requested branch lineage
// number of entries that will be retrieved for each branch is limitted to branchBatchSize (The reason it is not enough
// to read a single row is that we may retrieve deleted entries or tombstones, that should be skipped.
// the requested commitID is passed to the base branch as is. each of the lineage branches gets the commit id from its lineage.
func buildBaseLevelQuery(baseBranchID int64, lineage []lineageCommit, branchBatchSize int,
	requestedCommitID CommitID, prefixLen int, endOfPrefixRange string) map[int64]sq.SelectBuilder {
	unionMap := make(map[int64]sq.SelectBuilder)
	unionMap[baseBranchID] = buildSingleBranchQuery(baseBranchID, branchBatchSize, requestedCommitID, prefixLen, endOfPrefixRange)
	for _, l := range lineage {
		unionMap[l.BranchID] = buildSingleBranchQuery(l.BranchID, branchBatchSize, l.CommitID, prefixLen, endOfPrefixRange)
	}
	return unionMap
}

//
// builds a query on a single branch of the lineage returning entries as they were at topCommitID.
// called mainly from "buildBaseLevelQuery" above.
// the other function that calls it is "getMoreRows" that needs entries for a single branch.
// topCommitId contains the requested commit for that branch. its implications:
// 1. entries where the minCommitId is more than the requested commit id will be filtered out
// 2. entries that were deleted after this commit (maxCommitId > topCommitId) will be considered
//    undeleted
func buildSingleBranchQuery(branchID int64, branchBatchSize int, topCommitID CommitID, prefixLen int, endOfPrefixRange string) sq.SelectBuilder {
	query := sq.Select("branch_id", "min_commit").
		Distinct().Options(" ON (branch_id,path)").
		Column("substr(path,?) as path_suffix", prefixLen+1).
		Column("CASE WHEN max_commit >= ? THEN ? ELSE max_commit END AS max_commit", topCommitID, MaxCommitID).
		From("catalog_entries").
		Where("branch_id = ?", branchID).
		Where("min_commit <= ?", topCommitID).
		Where("path < ?", endOfPrefixRange).
		OrderBy("branch_id", "path", "min_commit desc").
		Limit(uint64(branchBatchSize))
	return query
}

// accept path listing results produced by "loopByLevel", and add entry details where the result is an entry
func loadEntriesIntoMarkerList(markerList []string, tx db.Tx, branchID int64, commitID CommitID, lineage []lineageCommit, delimiter, prefix string) ([]*catalog.Entry, error) {
	type entryRun struct {
		startRunIndex, runLength   int
		startEntryRun, endEntryRun string
	}
	var entryRuns []entryRun
	var inRun bool
	var previousInRun string
	var run entryRun
	for i, p := range markerList {
		// terminating by '/'(slash) character is an indication of a directory
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
	entries := make([]*catalog.Entry, len(markerList))
	entriesReader := sqEntriesLineageV(branchID, commitID, lineage)
	for _, r := range entryRuns {
		entriesSQL, args, err := sq.
			Select("path", "physical_address", "creation_date", "size", "checksum", "metadata").
			Where("NOT is_deleted AND path between ? and ?", prefix+r.startEntryRun, prefix+r.endEntryRun).
			FromSelect(entriesReader, "e").
			PlaceholderFormat(sq.Dollar).
			ToSql()
		if err != nil {
			return nil, fmt.Errorf("build entries sql: %w", err)
		}
		var entriesList []catalog.Entry
		err = tx.Select(&entriesList, entriesSQL, args...)
		if err != nil {
			return nil, fmt.Errorf("select entries: %w", err)
		}
		if len(entriesList) != r.runLength {
			return nil, fmt.Errorf("%w: read %d entries, got %d", catalog.ErrUnexpected, r.runLength, len(entriesList))
		}
		for i := 0; i < r.runLength; i++ {
			entries[r.startRunIndex+i] = &entriesList[i]
		}
	}
	// all the rest are common level items
	for i, p := range markerList {
		if entries[i] == nil {
			entries[i] = &catalog.Entry{
				CommonLevel: true,
				Path:        prefix + p,
			}
		}
	}
	return entries, nil
}
