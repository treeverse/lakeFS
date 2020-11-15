package catalog

import (
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
	minCommitID  CommitID
	commitsWhere sq.Sqlizer
	buf          []*DBScannerEntry
	idx          int
	after        string
	ended        bool
	err          error
	value        *DBScannerEntry
}

func NewDBBranchScanner(tx db.Tx, branchID int64, commitID, minCommitID CommitID, opts DBScannerOptions) *DBBranchScanner {
	s := &DBBranchScanner{
		tx:          tx,
		branchID:    branchID,
		idx:         0,
		commitID:    commitID,
		minCommitID: minCommitID,
		opts:        opts,
		after:       opts.After,
	}
	if s.opts.BufferSize == 0 {
		s.opts.BufferSize = DBScannerDefaultBufferSize
	}
	s.buf = make([]*DBScannerEntry, 0, s.opts.BufferSize)
	s.commitsWhere, s.err = s.buildCommitsWherePart()
	return s
}

func (s *DBBranchScanner) SetAdditionalWhere(part sq.Sqlizer) {
	s.opts.AdditionalWhere = part
}

func (s *DBBranchScanner) buildCommitsWherePart() (sq.Sqlizer, error) {
	if s.commitID == UncommittedID {
		return nil, nil
	}
	var branchMaxCommitID CommitID
	if s.commitID == CommittedID {
		branchMaxCommitID = MaxCommitID
	} else {
		branchMaxCommitID = s.commitID
	}
	// commit_id name is changed so that sorting will be performed on the numeric value, not the string value (where "10" is less than "2")
	var commits []int64
	sql := "SELECT commit_id FROM catalog_commits WHERE branch_id = $1 AND commit_id BETWEEN $2 AND $3 ORDER BY commit_id LIMIT $4"
	err := s.tx.Select(&commits, sql, s.branchID, s.minCommitID+1, branchMaxCommitID, BranchScannerMaxCommitsInFilter+1)
	if err != nil {
		return nil, err
	}
	if s.commitID == UncommittedID {
		commits = append(commits, int64(MaxCommitID))
	}
	if len(commits) == 0 {
		// this will actually never happen, since each branch has an initial branch
		commits = []int64{0}
	}

	var wherePart sq.Sqlizer
	if len(commits) <= BranchScannerMaxCommitsInFilter {
		return sq.Or{
			sq.Eq{"min_commit": commits},
			sq.Eq{"max_commit": append(commits, int64(s.minCommitID))},
		}, nil
	}

	upperCommitID := commits[len(commits)-1]
	wherePart = sq.Or{
		sq.Expr("min_commit BETWEEN ? AND ?", s.minCommitID+1, upperCommitID),
		sq.Expr("max_commit BETWEEN ? AND ?", s.minCommitID, upperCommitID),
	}
	return wherePart, nil
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
	if s.commitsWhere != nil {
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
