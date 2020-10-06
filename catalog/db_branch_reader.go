package catalog

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

type DBBranchReader struct {
	tx       db.Tx
	branchID int64
	buf      []*DBReaderEntry
	bufSize  int
	idx      int
	EOF      bool
	after    string
	commitID CommitID
	err      error
	value    *DBReaderEntry
}

func NewDBBranchReader(tx db.Tx, branchID int64, commitID CommitID, bufSize int, after string) *DBBranchReader {
	return &DBBranchReader{
		tx:       tx,
		branchID: branchID,
		buf:      make([]*DBReaderEntry, 0, bufSize),
		bufSize:  bufSize,
		after:    after,
		idx:      0,
		commitID: commitID,
	}
}

func (r *DBBranchReader) shouldAlignMaxCommit() bool {
	return r.commitID != CommittedID && r.commitID != UncommittedID
}

func (r *DBBranchReader) Next() bool {
	if r.EOF || r.err != nil {
		return false
	}
	if !r.readBuffer() {
		return false
	}
	r.value = r.buf[r.idx]
	// if entry was deleted after the max commit that can be read, it must be set to undeleted
	if r.shouldAlignMaxCommit() && r.value.MaxCommit >= r.commitID {
		r.value.MaxCommit = MaxCommitID
	}
	r.after = r.value.Path
	r.idx++
	return true
}

func (r *DBBranchReader) Err() error {
	return r.err
}

func (r *DBBranchReader) Value() *DBReaderEntry {
	if r.EOF || r.err != nil {
		return nil
	}
	return r.value
}

func (r *DBBranchReader) readBuffer() bool {
	if r.idx < len(r.buf) {
		return true
	}
	r.idx = 0
	r.buf = r.buf[:0]
	q := sqBranchReaderSelectWithCommitID(r.branchID, r.commitID).
		Limit(uint64(r.bufSize)).
		Where("path > ?", r.after)
	if sql, args, err := q.PlaceholderFormat(sq.Dollar).ToSql(); err != nil {
		r.err = err
	} else {
		r.err = r.tx.Select(&r.buf, sql, args...)
	}
	if len(r.buf) == 0 {
		r.EOF = true
		return false
	}
	return r.err == nil
}

func sqBranchReaderSelectWithCommitID(branchID int64, commitID CommitID) sq.SelectBuilder {
	q := sq.Select("branch_id", "path", "min_commit", "max_commit", "ctid").
		Distinct().Options(" ON (branch_id,path)").
		From("catalog_entries").
		Where("branch_id = ?", branchID).
		OrderBy("branch_id", "path", "min_commit desc")
	if commitID == CommittedID {
		q = q.Where("min_commit < ?", MaxCommitID)
	} else if commitID > 0 {
		q = q.Where("min_commit between 1 and ?", commitID)
	}
	return q
}
