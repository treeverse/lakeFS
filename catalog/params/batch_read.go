package params

import "time"

type BatchRead struct {
	ReadEntryMaxWait  time.Duration
	ScanTimeout       time.Duration
	BatchDelay        time.Duration
	EntriesReadAtOnce int
	Readers           int
}
