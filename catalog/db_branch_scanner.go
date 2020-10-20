package catalog

import (
	"strconv"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

const (
	DBScannerDefaultBufferSize = 1024
	MaxCommitsInFilter         = 100
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

func NewDBBranchScanner(tx db.Tx, branchID int64, commitID CommitID, opts *DBScannerOptions) *DBBranchScanner {
	s := &DBBranchScanner{
		tx:       tx,
		branchID: branchID,
		idx:      0,
		commitID: commitID,
	}
	if opts != nil {
		s.opts = *opts
		s.after = opts.After
	}
	if s.opts.BufferSize == 0 {
		s.opts.BufferSize = DBScannerDefaultBufferSize
	}
	s.buf = make([]*DBScannerEntry, 0, s.opts.BufferSize)
	commitsWhere, _ := getRelevantCommitsCondition(tx, branchID, commitID)
	s.commitsWhere = commitsWhere
	return s
}

func getRelevantCommitsCondition(tx db.Tx, branchID int64, commitID CommitID) (string, error) {
	var maxCommitId CommitID
	var commits []string
	var commitsWhere string
	if commitID == UncommittedID || commitID == CommittedID {
		maxCommitId = MaxCommitID
	} else {
		maxCommitId = commitID
	}
	// commit_id name is changed so that sorting will be performed on the numeric value, not the string value (where "10" is less than "2")
	sql := "SELECT commit_id::text as str_commit_id FROM catalog_commits WHERE branch_id = $1 AND commit_id <= $2 ORDER BY commit_id"
	err := tx.Select(&commits, sql, branchID, maxCommitId)
	if err != nil {
		panic(err)
	}
	if commitID == UncommittedID {
		commits = append(commits, strconv.FormatInt(int64(MaxCommitID), 10))
	}
	if len(commits) < MaxCommitsInFilter {
		commitsWhere = "min_commit in (" + strings.Join(commits, `,`) + ")"
	} else {
		commitsWhere = "min_commit BETWEEN 1 AND " + commits[len(commits)-1]
	}
	return commitsWhere, nil
}

func (s *DBBranchScanner) Next() bool {
	if s.hasEnded() {
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
	query, args, s.err = q.PlaceholderFormat(sq.Dollar).ToSql()
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
		Where(s.commitsWhere).
		OrderBy("branch_id", "path", "min_commit desc").
		Limit(uint64(s.opts.BufferSize))
	if s.after != "" {
		q = q.Where("path > ?", s.after)
	}
	//if s.commitID == CommittedID {
	//	q = q.Where("min_commit < ?", MaxCommitID)
	//} else if s.commitID > 0 {
	//	q = q.Where("min_commit between 1 and ?", s.commitID)
	//}
	if len(s.opts.AdditionalFields) > 0 {
		q = q.Columns(s.opts.AdditionalFields...)
	}
	if s.opts.AdditionalWhere != nil {
		q = q.Where(s.opts.AdditionalWhere)
	}
	return q
}
