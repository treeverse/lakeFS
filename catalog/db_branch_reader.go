package catalog

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

type DBBranchReader struct {
	tx       db.Tx
	branchID int64
	buf      []*DBReaderEntry
	bufSize  int
	EOF      bool
	after    string
	commitID CommitID
}

func NewDBBranchReader(tx db.Tx, branchID int64, commitID CommitID, bufSize int, after string) *DBBranchReader {
	return &DBBranchReader{
		tx:       tx,
		branchID: branchID,
		bufSize:  bufSize,
		after:    after,
		commitID: commitID,
	}
}

func (r *DBBranchReader) Next() (*DBReaderEntry, error) {
	if r.EOF {
		return nil, nil
	}
	if r.buf == nil {
		r.buf = make([]*DBReaderEntry, 0, r.bufSize)
		q := sqBranchReaderSelectWithCommitID(r.branchID, r.commitID).Limit(uint64(r.bufSize)).Where("path > ?", r.after)
		sql, args, err := q.PlaceholderFormat(sq.Dollar).ToSql()
		if err != nil {
			return nil, fmt.Errorf("next query format: %w", err)
		}
		err = r.tx.Select(&r.buf, sql, args...)
		if err != nil {
			return nil, fmt.Errorf("next select: %w", err)
		}
	}
	// returns the significant entry of that Path, and remove rows with that Path from buf
	l := len(r.buf)
	if l == 0 {
		r.EOF = true
		return nil, nil
	}
	// last Path in buffer may have more rows that were not read yet
	if r.buf[l-1].Path == r.buf[0].Path {
		err := r.extendBuf()
		if err != nil {
			return nil, fmt.Errorf("error getting entry : %w", err)
		}
		l = len(r.buf)
	}
	if l == 0 {
		r.EOF = true
		return nil, nil
	}
	firstPath := r.buf[0].Path
	i := 1
	for i < l && r.buf[i].Path == firstPath {
		i++
	}
	nextPK := findSignificantEntry(r.buf[:i], r.commitID)
	r.buf = r.buf[i:] // discard first rows from buffer
	return nextPK, nil
}

func findSignificantEntry(buf []*DBReaderEntry, lineageCommitID CommitID) *DBReaderEntry {
	l := len(buf)
	var ret *DBReaderEntry
	if buf[l-1].MinCommit == 0 { // uncommitted.Will appear only when reading includes uncommitted entries
		ret = buf[l-1]
	} else {
		ret = buf[0]
	}
	// if entry was deleted after the max commit that can be read, it must be set to undeleted
	if lineageCommitID == CommittedID ||
		lineageCommitID == UncommittedID ||
		ret.MaxCommit == MaxCommitID {
		return ret
	}
	if ret.MaxCommit >= lineageCommitID {
		ret.MaxCommit = MaxCommitID
	}
	return ret
}

func sqBranchReaderSelectWithCommitID(branchID int64, commitID CommitID) sq.SelectBuilder {
	q := sq.Select("branch_id", "path", "min_commit", "max_commit", "ctid").
		From("catalog_entries").
		Where("branch_id = ? ", branchID).
		OrderBy("branch_id", "path", "min_commit desc")
	if commitID == CommittedID {
		q = q.Where("min_commit > 0")
	} else if commitID > 0 {
		q = q.Where("min_commit between 1 and ?", commitID)
	}
	return q
}

func (r *DBBranchReader) extendBuf() error {
	l := len(r.buf)
	if l == 0 {
		return fmt.Errorf("empty buffer - internal error : %w", ErrUnexpected)
	}
	lastRow := r.buf[l-1]
	completionQuery := sqBranchReaderSelectWithCommitID(r.branchID, r.commitID)
	completionQuery = completionQuery.Where("Path = ? and min_commit < ?", lastRow.Path, lastRow.MinCommit)
	continuationQuery := sqBranchReaderSelectWithCommitID(r.branchID, r.commitID).
		Where("Path > ?", lastRow.Path).
		Limit(uint64(r.bufSize))
	unionQuery := completionQuery.
		Prefix("(").
		SuffixExpr(sq.ConcatExpr(")\n UNION ALL \n(", continuationQuery, ")"))
	sql, args, err := unionQuery.PlaceholderFormat(sq.Dollar).ToSql()
	if err != nil {
		return fmt.Errorf("format union query: %w", err)
	}
	return r.tx.Select(&r.buf, sql, args...)
}
