package catalog

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

const DBScannerDefaultBufferSize = 1024

type DBBranchScanner struct {
	opts     DBScannerOptions
	tx       db.Tx
	branchID int64
	commitID CommitID
	buf      []*DBScannerEntry
	idx      int
	after    string
	ended    bool
	err      error
	value    *DBScannerEntry
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
	return s
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
		OrderBy("branch_id", "path", "min_commit desc").
		Limit(uint64(s.opts.BufferSize))
	if s.after != "" {
		q = q.Where("path > ?", s.after)
	}
	if s.commitID == CommittedID {
		q = q.Where("min_commit < ?", MaxCommitID)
	} else if s.commitID > 0 {
		q = q.Where("min_commit between 1 and ?", s.commitID)
	}
	if len(s.opts.AdditionalFields) > 0 {
		q = q.Columns(s.opts.AdditionalFields...)
	}
	if s.opts.AdditionalWhere != nil {
		q = q.Where(s.opts.AdditionalWhere)
	}
	return q
}
