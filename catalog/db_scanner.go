package catalog

type DBScannerOptions struct {
	BufferSize       int
	FilterDeleted    bool
	After            string
	AdditionalFields []string
}

type DBScanner interface {
	Next() bool
	Value() *DBScannerEntry
	Err() error
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
