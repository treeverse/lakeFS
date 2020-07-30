package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

const (
	ListEntriesMaxLimit        = 10000
	ListEntriesBranchBatchSize = 32
)

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
		var entries []*Entry
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

type resultRow struct {
	BranchID   int64    `db:"branch_id"`
	PathSuffix string   `db:"path_postfix"`
	MinCommit  CommitID `db:"min_commit"`
	MaxCommit  CommitID `db:"max_commit"`
}

// reading is mainly done in loopByLevel. It may happen (hopefully rarely) in getMoreRows.
// variables needed for accessing the BD are packed and passed down to getMoreRows
type readPramsType struct {
	tx                          db.Tx
	prefix                      string
	branchBatchSize             int
	lineage                     []lineageCommit
	lowestCommitID, topCommitID CommitID
	branchID                    int64
}

func loopByLevel(tx db.Tx, prefix, after, delimiter string, limit, branchBatchSize int, branchID int64, requestedCommit CommitID, lineage []lineageCommit) ([]string, error) {
	// translate logical (uncommitted and committed) commit id to actual minCommit,maxCommit numbers in Rows
	lowestCommitID := CommitID(1)
	if requestedCommit == UncommittedID {
		lowestCommitID = UncommittedID
	}
	topCommitID := requestedCommit
	if requestedCommit <= UncommittedID {
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

	var listAfter string
	if len(after) == 0 {
		listAfter = prefix
	} else {
		listAfter = after
	}
	var markerList []string
	readParams := readPramsType{
		tx:              tx,
		prefix:          prefix,
		branchBatchSize: branchBatchSize,
		lineage:         lineage,
		lowestCommitID:  lowestCommitID,
		topCommitID:     topCommitID,
		branchID:        branchID,
	}
	first := true
	for {
		var pathCond string
		if first {
			first = false
			pathCond = ">="
		} else {
			pathCond = ">"
		}
		unionSelect := unionQueryParts[0].Where("path "+pathCond+" ? and path < ?", listAfter, endOfPrefixRange).Prefix("(").Suffix(")")
		for j := 1; j < len(lineage)+1; j++ {
			// add the path condition to each union part
			unionSelect = unionSelect.SuffixExpr(sq.ConcatExpr("\n UNION ALL \n", "(",
				unionQueryParts[j].Where("path "+pathCond+" ? and path < ?", listAfter, endOfPrefixRange), ")"))
		}
		fullQuery := sq.Select("*").FromSelect(unionSelect, "u")
		unionSQL, args, err := fullQuery.PlaceholderFormat(sq.Dollar).ToSql()
		if err != nil {
			return nil, err
		}
		resultRows := make([]resultRow, 0, branchBatchSize*len(lineage)+1)
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

func processSinglePrefix(response []resultRow, delimiter string, branchPriorityMap map[int64]int, limit int, readParams readPramsType) []string {
	// split results by branch
	branchRanges := make(map[int64][]resultRow, len(branchPriorityMap))
	for _, result := range response {
		b := result.BranchID
		_, exists := branchRanges[b]
		if !exists {
			branchRanges[b] = make([]resultRow, 0, readParams.branchBatchSize)
		}
		branchRanges[b] = append(branchRanges[b], result)
	}
	var resultPaths []string
	for { // exit loop by return
		b := findLowestResultInBranches(branchRanges, branchPriorityMap)
		p := branchRanges[b][0].PathSuffix
		pathResults := getBranchResultRowsForPath(p, b, branchRanges, readParams)
		if checkPathNotDeleted(pathResults) { // minimal path was found
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
		// if the path is not at the start of a result array - nothing happens
		for branch := range branchRanges {
			if branch != b {
				getBranchResultRowsForPath(p, branch, branchRanges, readParams)
			}
		}
		if len(branchRanges) == 0 { // no more to read
			return resultPaths
		}
	}
}

func getBranchResultRowsForPath(path string, branch int64, branchRanges map[int64][]resultRow, readParams readPramsType) []resultRow {
	i := 0
	resultLen := len(branchRanges[branch])
	for branchRanges[branch][i].PathSuffix == path {
		i++
		if i == resultLen {
			err := getMoreRows(path, branch, branchRanges, readParams)
			if err != nil { // assume that no more entries for this branch. so it is removed from branchRanges
				returnSlice := branchRanges[branch]
				delete(branchRanges, branch)
				return returnSlice
			}
			i = 0
			resultLen = len(branchRanges[branch])
		}
	}
	returnSlice := branchRanges[branch][:i]
	branchRanges[branch] = branchRanges[branch][i:]
	return returnSlice
}

func getMoreRows(path string, branch int64, branchRanges map[int64][]resultRow, readParams readPramsType) error {
	var topCommitID CommitID
	if branch == readParams.branchID { // it is the base branch
		topCommitID = readParams.topCommitID
	} else {
		for _, l := range readParams.lineage {
			if branch == l.BranchID {
				topCommitID = l.CommitID
				break
			}
		}
	}
	// have to re-read the last entry, because otherwise the expression becomes complex and the optimizer gets NUTS
	//so read size must be the batch size + whatever results were left from the last entry
	// If readBuf is not extended - there will be an endless loop if number of results is bigger than batch size.
	requiredBufferSize := readParams.branchBatchSize + len(branchRanges[branch])
	readBuf := make([]resultRow, 0, requiredBufferSize)
	singleSelect := selectSingleBranch(branch, branch == readParams.branchID, requiredBufferSize, readParams.lowestCommitID, topCommitID, len(readParams.prefix))
	requestedPath := readParams.prefix + path
	singleSelect = singleSelect.Where("path >= ? and path < ?", requestedPath, readParams.prefix+DirectoryTermination)
	s, args, err := singleSelect.PlaceholderFormat(sq.Dollar).ToSql()
	if err != nil {
		return err
	}

	err = readParams.tx.Select(&readBuf, s, args...)
	if len(branchRanges[branch]) == len(readBuf) {
		err = sql.ErrNoRows
	}
	if err != nil {
		return err
	}
	branchRanges[branch] = readBuf
	return nil
}

func findLowestResultInBranches(branchRanges map[int64][]resultRow, branchPriorityMap map[int64]int) int64 {
	firstTime := true
	var chosenBranch int64
	var chosenPath string
	for b, r := range branchRanges {
		if firstTime {
			firstTime = false
			chosenBranch = b
			chosenPath = r[0].PathSuffix
			continue
		}
		if r[0].PathSuffix == chosenPath {
			if branchPriorityMap[chosenBranch] > branchPriorityMap[b] {
				chosenBranch = b
				chosenPath = r[0].PathSuffix
			}
		} else if r[0].PathSuffix < chosenPath {
			chosenBranch = b
			chosenPath = r[0].PathSuffix
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

func buildBaseLevelQuery(baseBranchID int64, lineage []lineageCommit, branchEntryLimit int, lowestCommitID, topCommitID CommitID, prefixLen int) []sq.SelectBuilder {
	unionParts := make([]sq.SelectBuilder, len(lineage)+1)
	unionParts[0] = selectSingleBranch(baseBranchID, true, branchEntryLimit, lowestCommitID, topCommitID, prefixLen)
	for i, l := range lineage {
		unionParts[i+1] = selectSingleBranch(l.BranchID, false, branchEntryLimit, 1, l.CommitID, prefixLen)
	}
	return unionParts
}

func selectSingleBranch(branchID int64, isBaseBranch bool, branchBatchSize int, lowestCommitID, topCommitID CommitID, prefixLen int) sq.SelectBuilder {
	rawSelect := sq.Select("branch_id", "min_commit").
		Column("substr(path,?) as path_postfix", prefixLen+1).
		From("catalog_entries").
		Where("branch_id = ?", branchID).
		Where("min_commit between ? and  ? ", lowestCommitID, topCommitID).
		OrderBy("branch_id", "path", "min_commit desc").
		Limit(uint64(branchBatchSize))
	var query sq.SelectBuilder
	if isBaseBranch {
		query = rawSelect.Column("max_commit")
	} else {
		query = rawSelect.
			Column("CASE WHEN max_commit >= ? THEN catalog_max_commit_id() ELSE max_commit END AS max_commit", topCommitID)
	}
	return query
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
	entries := make([]*Entry, len(markerList))
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
		var entriesList []Entry
		err = tx.Select(&entriesList, entriesSQL, args...)
		if err != nil {
			return nil, fmt.Errorf("select entries: %w", err)
		}
		if len(entriesList) != r.runLength {
			return nil, fmt.Errorf("expect to read %d entries, got %d", r.runLength, len(entriesList))
		}
		for i := 0; i < r.runLength; i++ {
			entries[r.startRunIndex+i] = &entriesList[i]
		}
	}
	// all the rest are common level items
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
