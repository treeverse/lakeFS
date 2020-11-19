package mvcc

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/catalog"
)

type DBScannerEntry struct {
	BranchID int64  `db:"branch_id"`
	RowCtid  string `db:"ctid"`
	catalog.MinMaxCommit
	catalog.Entry
}

type DBScannerOptions struct {
	BufferSize       int
	After            string
	AdditionalFields []string
	AdditionalWhere  sq.Sqlizer
}

type DBScanner interface {
	Next() bool
	Value() *DBScannerEntry
	Err() error
	SetAdditionalWhere(s sq.Sqlizer)
}

func ScanDBEntriesUntil(s DBScanner, p string) error {
	ent := s.Value()
	for ent == nil || ent.Path < p {
		if !s.Next() {
			return s.Err()
		}
		ent = s.Value()
	}
	return nil
}
