package catalog

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

type DBBranchReader struct {
	tx                 db.Tx
	branchID           int64
	initialBuf         []*DBReaderEntry
	buf                []*DBReaderEntry
	bufSize            int
	EOF                bool
	after              string
	commitID           CommitID
}

func NewDBBranchReader(tx db.Tx, branchID int64, commitID CommitID, bufSize int, after string) *DBBranchReader {
	return &DBBranchReader{
		tx:         tx,
		branchID:   branchID,
		bufSize:    bufSize,
		initialBuf: make([]*DBReaderEntry, 0, bufSize),
		after:      after,
		commitID:   commitID,
	}
}

func (r *DBBranchReader) shouldAlignMaxCommit() bool {
	return r.commitID != CommittedID && r.commitID != UncommittedID
}

func (r *DBBranchReader) Next() (*DBReaderEntry, error) {
	if r.EOF {
		return nil, nil
	}
	if len(r.buf) == 0 {
		r.buf = r.initialBuf
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
	if len(r.buf) == 0 {
		r.EOF = true
		return nil, nil
	}
	nextPk := r.buf[0]
	// if entry was deleted after the max commit that can be read, it must be set to undeleted
	if r.shouldAlignMaxCommit() && nextPk.MaxCommit >= r.commitID {
		nextPk.MaxCommit = MaxCommitID
	}
	r.buf = r.buf[1:]
	r.after = nextPk.Path
	return nextPk, nil
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
