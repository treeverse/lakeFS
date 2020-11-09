package catalog

import sq "github.com/Masterminds/squirrel"

type DBScannerOptions struct {
	BufferSize       int
	After            string
	AdditionalFields []string
	AdditionalWhere  sq.Sqlizer
	TrimmedLineage   []lineageCommit
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
