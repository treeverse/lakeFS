package params

import "time"

type Cache struct {
	Enabled bool
	Size    int
	Expiry  time.Duration
	Jitter  time.Duration
}

type BatchRead struct {
	EntryMaxWait  time.Duration
	ScanTimeout   time.Duration
	Delay         time.Duration
	EntriesAtOnce int
	Readers       int
}

type BatchWrite struct {
	EntriesInsertSize int
}

type Catalog struct {
	BatchRead  BatchRead
	BatchWrite BatchWrite
	Cache      Cache
}
