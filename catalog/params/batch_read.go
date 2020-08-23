package params

type BatchRead struct {
	ReadEntryMaxWaitSec int
	ScanTimeoutMicroSec int
	BatchDelayMicroSec  int
	EntriesReadAtOnce   int
	ReadersNum          int
}
