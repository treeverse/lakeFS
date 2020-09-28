package catalog

//todo: reading logic will change once we change umcommitted indication in "min_commit" from 0 to MaxCommitID

import (
	"fmt"
	"math"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

type entryPK struct {
	BranchID  int64    `db:"branch_id"`
	Path      string   `db:"path"`
	MinCommit CommitID `db:"min_commit"`
	MaxCommit CommitID `db:"max_commit"`
	RowCtid   string   `db:"ctid"`
}

type singleBranchReader struct {
	tx        db.Tx
	branchID  int64
	buf       []*entryPK
	bufSize   int
	EOF       bool
	after     string
	commitID  CommitID
	firstTime bool
}

type lineageReader struct {
	tx           db.Tx
	branchID     int64
	EOF          bool
	commitID     CommitID
	readers      []*singleBranchReader
	nextRow      []*entryPK
	firstTime    bool
	limit        int
	returnedRows int
}

func NewSingleBranchReader(tx db.Tx, branchID int64, commitID CommitID, bufSize int, after string) *singleBranchReader {
	return &singleBranchReader{
		tx:        tx,
		branchID:  branchID,
		bufSize:   bufSize,
		after:     after,
		commitID:  commitID,
		firstTime: true,
	}
}

func NewLineageReader(tx db.Tx, branchID int64, commitID CommitID, bufSize, limit int, after string) (*lineageReader, error) {
	// limit <= 0 means there is no limit to number of returned rows
	lineage, err := getLineage(tx, branchID, commitID)
	if err != nil {
		return nil, fmt.Errorf("error getting lineage : %w", err)
	}
	if limit <= 0 { // unlimited
		limit = math.MaxInt32
	}
	lr := &lineageReader{
		tx:        tx,
		branchID:  branchID,
		commitID:  commitID,
		firstTime: true,
		readers:   make([]*singleBranchReader, len(lineage)+1),
		limit:     limit,
	}
	lr.readers[0] = NewSingleBranchReader(tx, branchID, commitID, bufSize, after)
	for i, bl := range lineage {
		lr.readers[i+1] = NewSingleBranchReader(tx, bl.BranchID, bl.CommitID, bufSize, after)
	}
	lr.nextRow = make([]*entryPK, len(lr.readers))
	return lr, nil
}

func (r *lineageReader) GetNextPK() (*entryPK, error) {
	if r.EOF {
		return nil, nil
	}
	if r.firstTime {
		r.firstTime = false
		for i, reader := range r.readers {
			e, err := reader.GetNextPK()
			if err != nil {
				return nil, fmt.Errorf("error getting entry from branch ID %d : %w", reader.branchID, err)
			}
			r.nextRow[i] = e
		}
	}
	var selectedEntry *entryPK
	// indirection array, to skip lineage branches that reached end
	nonNilNextRow := make([]int, 0, len(r.nextRow))
	for i, ent := range r.nextRow {
		if ent != nil {
			nonNilNextRow = append(nonNilNextRow, i)
		}
	}
	if len(nonNilNextRow) == 0 {
		r.EOF = true
		return nil, nil
	}
	// find lowest Path
	selectedEntry = r.nextRow[nonNilNextRow[0]]
	for i := 1; i < len(nonNilNextRow); i++ {
		if selectedEntry.Path > r.nextRow[nonNilNextRow[i]].Path {
			selectedEntry = r.nextRow[nonNilNextRow[i]]
		}
	}
	r.returnedRows++
	if r.returnedRows >= r.limit {
		r.EOF = true
	}
	// advance next row for all branches that have this Path
	for i := 0; i < len(nonNilNextRow); i++ {
		if r.nextRow[nonNilNextRow[i]].Path == selectedEntry.Path {
			n, err := r.readers[nonNilNextRow[i]].GetNextPK()
			if err != nil {
				return nil, fmt.Errorf("error getting entry on branch : %w", err)
			}
			r.nextRow[nonNilNextRow[i]] = n
		}
	}
	return selectedEntry, nil
}

func (r *singleBranchReader) GetNextPK() (*entryPK, error) {
	if r.EOF {
		return nil, nil
	}
	if r.firstTime {
		r.firstTime = false
		r.buf = make([]*entryPK, 0, r.bufSize)
		q := branchSelectWithCommitID(r.branchID, r.commitID).Limit(uint64(r.bufSize)).Where("path > ?", r.after)
		err := readEntriesIntoBuf(r.tx, q, &r.buf)
		if err != nil {
			return nil, fmt.Errorf("error getting entry : %w", err)
		}
	}
	// returnes the significant entry of that Path, and remove rows with that Path from buf
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
	var i int
	for i = 1; i < l && r.buf[i].Path == firstPath; i++ {
	}
	nextPK := findSignificantEntry(r.buf[:i], r.commitID)
	r.buf = r.buf[i:] // discard first rows from buffer
	return nextPK, nil
}

func findSignificantEntry(buf []*entryPK, lineageCommitID CommitID) *entryPK {
	var ret *entryPK
	l := len(buf)
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
	//todo: rethink condition
	if ret.MaxCommit >= lineageCommitID {
		ret.MaxCommit = MaxCommitID
	}
	return ret
}

func branchSelectWithCommitID(branchID int64, commitID CommitID) sq.SelectBuilder {
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

func (r *singleBranchReader) extendBuf() error {
	l := len(r.buf)
	if l == 0 {
		return fmt.Errorf("empty buffer - internal error : %w", ErrUnexpected)
	}
	lastRow := r.buf[l-1]
	completionQuery := branchSelectWithCommitID(r.branchID, r.commitID)
	completionQuery = completionQuery.Where("Path = ? and min_commit < ?", lastRow.Path, lastRow.MinCommit)
	continuationQuery := branchSelectWithCommitID(r.branchID, r.commitID).
		Where("Path > ?", lastRow.Path).
		Limit(uint64(r.bufSize))
	// move rows of last Path to beginning of buffer
	tempBuf := make([]*entryPK, 0, r.bufSize+len(r.buf)*2)
	tempBuf = append(tempBuf, r.buf...)
	r.buf = tempBuf
	unionQuery := completionQuery.
		Prefix("(").
		SuffixExpr(sq.ConcatExpr(")\n UNION ALL \n(", continuationQuery, ")"))
	err := readEntriesIntoBuf(r.tx, unionQuery, &r.buf)
	return err
}

func readEntriesIntoBuf(tx db.Tx, q sq.SelectBuilder, buf *[]*entryPK) error {
	sql, args, err := q.PlaceholderFormat(sq.Dollar).ToSql()
	if err != nil {
		return err
	}
	err = tx.Select(buf, sql, args...)
	return err
}
