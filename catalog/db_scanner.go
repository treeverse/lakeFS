package catalog

import sq "github.com/Masterminds/squirrel"

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
}

func ScanDBEntriesUntil(s DBScanner, p string) (*DBScannerEntry, error) {
	for s.Value() == nil || s.Value().Path < p {
		if !s.Next() {
			return nil, s.Err()
		}
	}
	return s.Value(), nil
}
