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

//type DBEntriesScanner interface {
//	Next() bool
//	Err() error
//	Value() *DBScannerEntry
//	hasEnded() bool
//	ReadLineage() ([]lineageCommit, error)
//}

func (s *DBLineageScanner) Next() bool {
	if s.ended {
		return false
	}
	if !s.ensureBranchScanners() {
		return false
	}

	// select lowest entry based on path
	var selectedEntry *DBScannerEntry
	for _, scanner := range s.scanners {
		ent := scanner.Value()
		if ent != nil && (selectedEntry == nil || selectedEntry.Path > ent.Path) {
			selectedEntry = ent
		}
	}
	s.value = selectedEntry

	// mark scanner ended if no new entry was selected
	if s.value == nil {
		s.ended = true
		return false
	}

	// move scanners to next item, in case they point to the same path as current element
	for _, scanner := range s.scanners {
		ent := scanner.Value()
		if ent == nil || ent.Path != s.value.Path {
			continue
		}
		if scanner.Next() {
			continue
		}
		if err := scanner.Err(); err != nil {
			s.err = fmt.Errorf("getting entry on branch: %w", err)
			return false
		}
	}
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
func (s *DBLineageScanner) getBranchID() int64 {
	return s.branchID
}
func (s *DBLineageScanner) hasEnded() bool {
	return s.ended || s.err != nil
}

func (s *DBLineageScanner) ReadLineage() ([]lineageCommit, error) {
	return getLineage(s.tx, s.branchID, s.commitID)
}

func (s *DBLineageScanner) ensureBranchScanners() bool {
	if s.scanners != nil {
		return true
	}
	lineage, err := s.ReadLineage()
	if err != nil {
		s.err = fmt.Errorf("getting lineage: %w", err)
		return false
	}
	s.scanners = make([]*DBBranchScanner, len(lineage)+1)
	s.scanners[0] = NewDBBranchScanner(s.tx, s.branchID, s.commitID, &s.opts)
	for i, bl := range lineage {
		s.scanners[i+1] = NewDBBranchScanner(s.tx, bl.BranchID, bl.CommitID, &s.opts)
	}
	for _, branchScanner := range s.scanners {
		if branchScanner.Next() {
			continue
		}
		if err := branchScanner.Err(); err != nil {
			s.err = fmt.Errorf("getting entry from branch ID %d: %w", branchScanner.branchID, err)
			return false
		}
	}
	return true
}
