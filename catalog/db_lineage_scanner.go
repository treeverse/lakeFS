package catalog

import (
	"fmt"

	"github.com/treeverse/lakefs/db"
)

type DBLineageScanner struct {
	tx       db.Tx
	branchID int64
	commitID CommitID
	scanners []*DBBranchScanner
	nextRow  []*DBScannerEntry
	ended    bool
	err      error
	value    *DBScannerEntry
	opts     DBScannerOptions
}

func NewDBLineageScanner(tx db.Tx, branchID int64, commitID CommitID, opts *DBScannerOptions) *DBLineageScanner {
	s := &DBLineageScanner{
		tx:       tx,
		branchID: branchID,
		commitID: commitID,
	}
	if opts != nil {
		s.opts = *opts
	}
	return s
}

func (s *DBLineageScanner) Next() bool {
	if s.ended {
		return false
	}
	if !s.ensureBranchScanners() {
		return false
	}

	// indirection array, to skip lineage branches that reached end
	nonNilNextRow := make([]int, 0, len(s.nextRow))
	for i, ent := range s.nextRow {
		if ent != nil {
			nonNilNextRow = append(nonNilNextRow, i)
		}
	}
	if len(nonNilNextRow) == 0 {
		s.ended = true
		return false
	}

	// find lowest Path
	selectedEntry := s.nextRow[nonNilNextRow[0]]
	for i := 1; i < len(nonNilNextRow); i++ {
		if selectedEntry.Path > s.nextRow[nonNilNextRow[i]].Path {
			selectedEntry = s.nextRow[nonNilNextRow[i]]
		}
	}

	// advance next row for all branches that have this Path
	for i := 0; i < len(nonNilNextRow); i++ {
		branchIdx := nonNilNextRow[i]
		if s.nextRow[branchIdx].Path == selectedEntry.Path {
			var ent *DBScannerEntry
			branchScanner := s.scanners[branchIdx]
			if branchScanner.Next() {
				ent = branchScanner.Value()
			} else if branchScanner.Err() != nil {
				s.err = fmt.Errorf("getting entry on branch: %w", branchScanner.Err())
				return false
			}
			s.nextRow[branchIdx] = ent
		}
	}
	s.value = selectedEntry
	return true
}

func (s *DBLineageScanner) Err() error {
	return s.err
}

func (s *DBLineageScanner) Value() *DBScannerEntry {
	if s.hasEnded() {
		return nil
	}
	return s.value
}

func (s *DBLineageScanner) hasEnded() bool {
	return s.ended || s.err != nil
}

func (s *DBLineageScanner) ensureBranchScanners() bool {
	if s.scanners != nil {
		return true
	}
	lineage, err := getLineage(s.tx, s.branchID, s.commitID)
	if err != nil {
		s.err = fmt.Errorf("error getting lineage: %w", err)
		return false
	}
	s.scanners = make([]*DBBranchScanner, len(lineage)+1)
	s.scanners[0] = NewDBBranchScanner(s.tx, s.branchID, s.commitID, &s.opts)
	for i, bl := range lineage {
		s.scanners[i+1] = NewDBBranchScanner(s.tx, bl.BranchID, bl.CommitID, &s.opts)
	}
	s.nextRow = make([]*DBScannerEntry, len(s.scanners))
	for i, branchScanner := range s.scanners {
		if branchScanner.Next() {
			s.nextRow[i] = branchScanner.Value()
		} else if branchScanner.Err() != nil {
			s.err = fmt.Errorf("getting entry from branch ID %d: %w", branchScanner.branchID, branchScanner.Err())
			return false
		}
	}
	return true
}
