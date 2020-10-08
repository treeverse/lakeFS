package catalog

type DBScannerOptions struct {
	BufferSize       int
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
		_ = s.Next()
		ent = s.Value()
		if err := s.Err(); err != nil {
			return nil, err
		}
		if ent == nil {
			return nil, nil
		}
	}
	return ent, nil
}
