package params

import "time"

type Catalog struct {
	// batch read entries
	ReadEntryMaxWait  time.Duration
	ReadScanTimeout   time.Duration
	ReadDelay         time.Duration
	ReadEntriesAtOnce int
	ReadReaders       int

	// create entries batch
	CreateEntriesInsertSize int

	// cache
	CacheEnabled bool
	CacheSize    int
	CacheExpiry  time.Duration
	CacheJitter  time.Duration
}
