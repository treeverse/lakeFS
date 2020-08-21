package params

type BatchReadParams struct {
	ReadEntryMaxWaitSec int
	ScanTimeoutMicroSec int
	BatchDelayMicroSec  int
	EntriesReadAtOnce   int
	ReadersNum          int
}
