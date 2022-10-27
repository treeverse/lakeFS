package committed

//go:generate mockgen -source=batch_write_closer.go -destination=mock/batch_write_closer.go -package=mock

// BatchWriterCloser collects Range writers and handles the asynchronous
// flushing and closing of the writers.
// Example usage:
//
//	func batch(manager RangeManager, bwc BatchWriterCloser) {
//			w1, _ := manager.GetWriter()
//			_ = w1.WriteRecord(graveler.ValueRecord{Key: "foo1", Value: &graveler.Value{Address: "bar1"}})
//			_ = w1.WriteRecord(graveler.ValueRecord{Key: "foo2", Value: &graveler.Value{Address: "bar2"}})
//			_ = bwc.CloseWriterAsync(w1)
//
//			w2, _ := manager.GetWriter()
//			_ = w2.WriteRecord(graveler.ValueRecord{Key: "goo1", Value: &graveler.Value{Address: "baz1"}})
//			_ = bwc.CloseWriterAsync(w2)
//
//			// blocks until all writers finished or any writer failed
//			res, err := bwc.Wait()
//			// handle err, results, etc..
//		}
type BatchWriterCloser interface {
	// CloseWriterAsync adds MetaRangeWriter instance for the BatchWriterCloser to handle.
	// Any writes executed to the writer after this call are not guaranteed to succeed.
	// If Wait() has already been called, returns an error.
	CloseWriterAsync(ResultCloser) error

	// Wait returns when all Writers finished.
	// Any failure to close a single MetaRangeWriter will return with a nil results slice and an error.
	Wait() ([]WriteResult, error)
}
