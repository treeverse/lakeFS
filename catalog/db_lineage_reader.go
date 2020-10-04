package catalog

import (
	"fmt"

	"github.com/treeverse/lakefs/db"
)

type DBLineageReader struct {
	tx           db.Tx
	branchID     int64
	EOF          bool
	commitID     CommitID
	readers      []*DBBranchReader
	nextRow      []*DBReaderEntry
	firstTime    bool
	limit        int
	returnedRows int
}

const (
	DBReaderMaxLimit = 1000
)

func NewDBLineageReader(tx db.Tx, branchID int64, commitID CommitID, bufSize int, limit int, after string) (*DBLineageReader, error) {
	lineage, err := getLineage(tx, branchID, commitID)
	if err != nil {
		return nil, fmt.Errorf("error getting lineage: %w", err)
	}
	if limit > DBReaderMaxLimit {
		limit = DBReaderMaxLimit
	}
	lr := &DBLineageReader{
		tx:        tx,
		branchID:  branchID,
		commitID:  commitID,
		firstTime: true,
		readers:   make([]*DBBranchReader, len(lineage)+1),
		limit:     limit,
	}
	lr.readers[0] = NewDBBranchReader(tx, branchID, commitID, bufSize, after)
	for i, bl := range lineage {
		lr.readers[i+1] = NewDBBranchReader(tx, bl.BranchID, bl.CommitID, bufSize, after)
	}
	lr.nextRow = make([]*DBReaderEntry, len(lr.readers))
	for i, reader := range lr.readers {
		e, err := reader.Next()
		if err != nil {
			return nil, fmt.Errorf("getting entry from branch ID %d: %w", reader.branchID, err)
		}
		lr.nextRow[i] = e
	}
	return lr, nil
}

func (r *DBLineageReader) Next() (*DBReaderEntry, error) {
	if r.EOF {
		return nil, nil
	}
	var selectedEntry *DBReaderEntry
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
	if r.limit >= 0 && r.returnedRows >= r.limit {
		r.EOF = true
	}
	// advance next row for all branches that have this Path
	for i := 0; i < len(nonNilNextRow); i++ {
		if r.nextRow[nonNilNextRow[i]].Path == selectedEntry.Path {
			n, err := r.readers[nonNilNextRow[i]].Next()
			if err != nil {
				return nil, fmt.Errorf("error getting entry on branch : %w", err)
			}
			r.nextRow[nonNilNextRow[i]] = n
		}
	}
	return selectedEntry, nil
}
