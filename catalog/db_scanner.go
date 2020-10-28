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
	getBranchID() int64
}

func ScanDBEntryUntil(s DBScanner, ent *DBScannerEntry, p string) (*DBScannerEntry, error) {
	for ent == nil || ent.Path < p {
		if !s.Next() {
			return nil, s.Err()
		}
		ent = s.Value()
	}
	return ent, nil
}
