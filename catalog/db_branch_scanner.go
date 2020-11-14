package catalog

import (
	"strconv"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

const (
	DBScannerDefaultBufferSize      = 4096
	BranchScannerMaxCommitsInFilter = 40
)

type DBBranchScanner struct {
	opts         DBScannerOptions
	tx           db.Tx
	branchID     int64
	commitID     CommitID
	commitsWhere string
	buf          []*DBScannerEntry
	idx          int
	after        string
	ended        bool
	err          error
	value        *DBScannerEntry
}

func NewDBBranchScanner(tx db.Tx, branchID int64, commitID, minMinCommit CommitID, opts DBScannerOptions) *DBBranchScanner {
	s := &DBBranchScanner{
		tx:       tx,
		branchID: branchID,
		idx:      0,
		commitID: commitID,
		opts:     opts,
		after:    opts.After,
	}
	if s.opts.BufferSize == 0 {
		s.opts.BufferSize = DBScannerDefaultBufferSize
	}
	s.buf = make([]*DBScannerEntry, 0, s.opts.BufferSize)
	commitsWhere, err := getRelevantCommitsCondition(tx, branchID, commitID, minMinCommit)
	s.err = err
	s.commitsWhere = commitsWhere
	return s
}

func (s *DBBranchScanner) SetAdditionalWhere(part sq.Sqlizer) {
	s.opts.AdditionalWhere = part
}

func getRelevantCommitsCondition(tx db.Tx, branchID int64, commitID, minMinCommit CommitID) (string, error) {
	var branchMaxCommitID CommitID
	var commits []string
	var commitsWhere string
	if commitID == UncommittedID {
		return "", nil
	}
	if commitID == UncommittedID || commitID == CommittedID {
		branchMaxCommitID = MaxCommitID
	} else {
		branchMaxCommitID = commitID
	}
	// commit_id name is changed so that sorting will be performed on the numeric value, not the string value (where "10" is less than "2")
	sql := "SELECT commit_id::text as str_commit_id FROM catalog_commits WHERE branch_id = $1 AND commit_id BETWEEN  $2 AND $3 ORDER BY commit_id limit $4"
	err := tx.Select(&commits, sql, branchID, minMinCommit+1, branchMaxCommitID, BranchScannerMaxCommitsInFilter+1)
	if err != nil {
		return "", err
	}
	if commitID == UncommittedID {
		commits = append(commits, strconv.FormatInt(int64(MaxCommitID), 10))
	}
	if len(commits) == 0 {
		commits = append(commits, "-1") // this will actually never happen, since each branch has an initial branch
		// anyway - there is no commit id -1
	}
	if len(commits) <= BranchScannerMaxCommitsInFilter {
		minCommitsWhere := "min_commit in (" + strings.Join(commits, `,`) + ")"
		commits = append(commits, strconv.FormatInt(int64(minMinCommit), 10)) // add the minimal commit, because if it appears in a max_commit, It is a change
		maxCommitWhere := "max_commit in (" + strings.Join(commits, `,`) + ")"
		commitsWhere = "(" + minCommitsWhere + " OR " + maxCommitWhere + ")"
	} else {
		commitsWhere = "(min_commit BETWEEN " + strconv.FormatInt(int64(minMinCommit)+1, 10) + " AND " + commits[len(commits)-1] +
			" OR max_commit BETWEEN " + strconv.FormatInt(int64(minMinCommit), 10) + " AND " + commits[len(commits)-1] + ")"
	}
	return commitsWhere, nil
}

func (s *DBBranchScanner) Next() bool {
	if s.hasEnded() {
		return false
	}
	if s.err != nil {
		return false
	}
	if !s.readBufferIfNeeded() {
		return false
	}
	s.value = s.buf[s.idx]
	// if entry was deleted after the max commit that can be read, it must be set to undeleted
	if s.shouldAlignMaxCommit() && s.value.MaxCommit >= s.commitID {
		s.value.MaxCommit = MaxCommitID
	}
	s.after = s.value.Path
	s.idx++
	return true
}

func (s *DBBranchScanner) Err() error {
	return s.err
}

func (s *DBBranchScanner) Value() *DBScannerEntry {
	if s.hasEnded() {
		return nil
	}
	return s.value
}

func (s *DBBranchScanner) hasEnded() bool {
	return s.ended || s.err != nil
}

func (s *DBBranchScanner) shouldAlignMaxCommit() bool {
	return s.commitID != CommittedID && s.commitID != UncommittedID
}

func (s *DBBranchScanner) readBufferIfNeeded() bool {
	if s.idx < len(s.buf) {
		return true
	}
	// start fresh
	s.idx = 0
	s.buf = s.buf[:0]
	// query entries
	var query string
	var args []interface{}
	q := s.buildQuery()
	query, args, s.err = q.ToSql()
	if s.err != nil {
		return false
	}
	s.err = s.tx.Select(&s.buf, query, args...)
	if s.err != nil {
		return false
	}
	// mark iterator ended if no results
	if len(s.buf) == 0 {
		s.ended = true
		return false
	}
	return true
}

func (s *DBBranchScanner) buildQuery() sq.SelectBuilder {
	q := sq.Select("branch_id", "path", "min_commit", "max_commit", "ctid").
		Distinct().Options(" ON (branch_id,path)").
		From("catalog_entries").
		Where("branch_id = ?", s.branchID).
		OrderBy("branch_id", "path", "min_commit desc").
		Limit(uint64(s.opts.BufferSize))
	if s.after != "" {
		q = q.Where("path > ?", s.after)
	}
	if s.commitsWhere != "" {
		q = q.Where(s.commitsWhere)
	}
	if len(s.opts.AdditionalFields) > 0 {
		q = q.Columns(s.opts.AdditionalFields...)
	}
	if s.opts.AdditionalWhere != nil {
		q = q.Where(s.opts.AdditionalWhere)
	}
	z := sq.DebugSqlizer(q)
	_ = z
	q = q.PlaceholderFormat(sq.Dollar)
	return q
}
