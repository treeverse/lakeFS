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
	returnedRows int
}

func NewDBLineageReader(tx db.Tx, branchID int64, commitID CommitID, bufSize int, after string) (*DBLineageReader, error) {
	lineage, err := getLineage(tx, branchID, commitID)
	if err != nil {
		return nil, fmt.Errorf("error getting lineage: %w", err)
	}
	lr := &DBLineageReader{
		tx:        tx,
		branchID:  branchID,
		commitID:  commitID,
		firstTime: true,
		readers:   make([]*DBBranchReader, len(lineage)+1),
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
	selectedEntry := r.nextRow[nonNilNextRow[0]]
	for i := 1; i < len(nonNilNextRow); i++ {
		branchIdx := nonNilNextRow[i]
		if selectedEntry.Path > r.nextRow[branchIdx].Path {
			selectedEntry = r.nextRow[branchIdx]
		}
	}
	r.returnedRows++

	// advance next row for all branches that have this Path
	for i := 0; i < len(nonNilNextRow); i++ {
		branchIdx := nonNilNextRow[i]
		if r.nextRow[branchIdx].Path == selectedEntry.Path {
			n, err := r.readers[branchIdx].Next()
			if err != nil {
				return nil, fmt.Errorf("entry on branch : %w", err)
			}
			r.nextRow[branchIdx] = n
		}
	}
	return selectedEntry, nil
}
