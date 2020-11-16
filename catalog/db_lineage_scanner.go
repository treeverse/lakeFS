package catalog

import (
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"

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
	opts     DBLineageScannerOptions
}

type DBLineageScannerOptions struct {
	DBScannerOptions
	Lineage    []lineageCommit
	MinCommits []CommitID
}

var ErrMinCommitsMismatch = errors.New("MinCommits length mismatch")

func NewDBLineageScanner(tx db.Tx, branchID int64, commitID CommitID, opts DBLineageScannerOptions) *DBLineageScanner {
	s := &DBLineageScanner{
		tx:       tx,
		branchID: branchID,
		commitID: commitID,
		opts:     opts,
	}
	s.buildScanners()
	return s
}

func (s *DBLineageScanner) SetAdditionalWhere(part sq.Sqlizer) {
	s.opts.AdditionalWhere = part
	if s.scanners != nil {
		for _, scanner := range s.scanners {
			scanner.SetAdditionalWhere(part)
		}
	}
}

func (s *DBLineageScanner) Next() bool {
	if s.ended {
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
func (s *DBLineageScanner) hasEnded() bool {
	return s.ended || s.err != nil
}

func (s *DBLineageScanner) ReadLineage() ([]lineageCommit, error) {
	return getLineage(s.tx, s.branchID, s.commitID)
}

func (s *DBLineageScanner) buildScanners() {
	var err error
	var lineage []lineageCommit
	if s.opts.Lineage != nil {
		lineage = s.opts.Lineage
	} else {
		lineage, err = s.ReadLineage()
	}
	if err != nil {
		s.err = fmt.Errorf("getting lineage: %w", err)
		return
	}
	s.scanners = make([]*DBBranchScanner, len(lineage)+1)
	// use min commits or allocate default
	minCommits := s.opts.MinCommits
	if minCommits == nil {
		minCommits = make([]CommitID, len(s.scanners))
		for i := 0; i < len(minCommits); i++ {
			minCommits[i] = 1
		}
	} else if len(minCommits) != len(s.scanners) {
		s.err = ErrMinCommitsMismatch
	}
	s.scanners[0] = NewDBBranchScanner(s.tx, s.branchID, s.commitID, minCommits[0], s.opts.DBScannerOptions)
	for i, bl := range lineage {
		s.scanners[i+1] = NewDBBranchScanner(s.tx, bl.BranchID, bl.CommitID, minCommits[i+1], s.opts.DBScannerOptions)
	}
	for _, branchScanner := range s.scanners {
		if branchScanner.Next() {
			continue
		}
		if err := branchScanner.Err(); err != nil {
			s.err = fmt.Errorf("getting entry from branch ID %d: %w", branchScanner.branchID, err)
			return
		}
	}
}
