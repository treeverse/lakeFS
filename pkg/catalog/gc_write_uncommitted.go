package catalog

import (
	"context"
	"strings"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

func gcWriteUncommitted(ctx context.Context, store Store, kvStore kv.Store, repository *graveler.RepositoryRecord, w *UncommittedWriter, mark *GCUncommittedMark, runID string, maxFileSize int64) (*GCUncommittedMark, bool, error) {
	pw, err := writer.NewParquetWriterFromWriter(w, new(UncommittedParquetObject), gcParquetParallelNum) // TODO: Play with np count
	if err != nil {
		return nil, false, err
	}
	pw.CompressionType = parquet.CompressionCodec_GZIP

	// write uncommitted data from branches
	var hasData bool
	if mark == nil || !mark.Tracked {
		mark, hasData, err = gcWriteUncommittedBranches(ctx, store, pw, repository, w, mark, runID, maxFileSize)
		if err != nil {
			return nil, false, err
		}
		// We like to return in case data is written data and not the final marker.
		// This will enable processing tracked addresses in case there was no uncommitted entries, or we are processing
		// the last chunk
		if hasData && mark != nil {
			return mark, true, nil
		}
	}
	// write tracked physical addresses
	return gcWriteUncommittedTracked(ctx, kvStore, pw, repository, w, mark.Key, runID, maxFileSize)
}

// gcWriteUncommittedBranches used by gcWriteUncommitted to write uncommitted entries by iterating over branches
func gcWriteUncommittedBranches(ctx context.Context, store Store, pw *writer.ParquetWriter, repository *graveler.RepositoryRecord, w *UncommittedWriter, mark *GCUncommittedMark, runID string, maxFileSize int64) (*GCUncommittedMark, bool, error) {
	it, err := NewUncommittedIterator(ctx, store, repository)
	if err != nil {
		return nil, false, err
	}
	defer it.Close()

	if mark != nil {
		it.SeekGE(mark.BranchID, mark.Path)
	}

	normalizedStorageNamespace := string(repository.StorageNamespace)
	if !strings.HasSuffix(normalizedStorageNamespace, DefaultPathDelimiter) {
		normalizedStorageNamespace += DefaultPathDelimiter
	}

	count := 0
	nextMark := &GCUncommittedMark{RunID: runID, Tracked: true}
	for it.Next() {
		entry := it.Value()
		// Skip if entry is tombstone
		if entry.Entry == nil {
			continue
		}
		// Skip non-relative that address outside the storage namespace
		entryAddress := entry.Address
		if entry.Entry.AddressType != Entry_RELATIVE {
			if !strings.HasPrefix(entry.Address, normalizedStorageNamespace) {
				continue
			}
			entryAddress = entryAddress[len(normalizedStorageNamespace):]
		}

		count += 1
		if count%gcPeriodicCheckSize == 0 {
			if err := pw.Flush(true); err != nil {
				return nil, false, err
			}
		}
		if w.Size() > maxFileSize {
			nextMark.BranchID = entry.branchID
			nextMark.Path = entry.Path
			nextMark.Tracked = false
			break
		}
		if err = pw.Write(UncommittedParquetObject{
			PhysicalAddress: entryAddress,
			CreationDate:    entry.LastModified.AsTime().Unix(),
		}); err != nil {
			return nil, false, err
		}
	}
	if err := it.Err(); err != nil {
		return nil, false, err
	}
	if err := pw.WriteStop(); err != nil {
		return nil, false, err
	}

	// Finished reading all staging area - return marker to switch processing tracked physical addresses
	hasData := count > 0
	return nextMark, hasData, nil
}

// gcWriteUncommittedTracked used by gcWriteUncommitted to write down tracked physical addresses
func gcWriteUncommittedTracked(ctx context.Context, kvStore kv.Store, pw *writer.ParquetWriter, repository *graveler.RepositoryRecord, w *UncommittedWriter, key string, runID string, maxFileSize int64) (*GCUncommittedMark, bool, error) {
	repoPartition := graveler.RepoPartition(repository)
	msgType := (&Entry{}).ProtoReflect().Type()
	prefix := []byte(kv.FormatPath(kvTrackPrefix, ""))
	it, err := kv.NewPrimaryIterator(ctx, kvStore, msgType, repoPartition, prefix, kv.IteratorOptionsFrom([]byte(key)))
	if err != nil {
		return nil, false, err
	}
	defer it.Close()

	var nextMark *GCUncommittedMark
	count := 0
	for it.Next() {
		itEntry := it.Entry()
		entry := itEntry.Value.(*Entry)
		count += 1
		if count%gcPeriodicCheckSize == 0 {
			if err := pw.Flush(true); err != nil {
				return nil, false, err
			}
		}
		if w.Size() > maxFileSize {
			// set mark to use for continuation
			nextMark = &GCUncommittedMark{
				RunID:   runID,
				Tracked: true,
				Key:     entry.Address,
			}
			break
		}
		if err = pw.Write(UncommittedParquetObject{
			PhysicalAddress: entry.Address,
			CreationDate:    entry.LastModified.AsTime().Unix(),
		}); err != nil {
			return nil, false, err
		}
	}
	if err := it.Err(); err != nil {
		return nil, false, err
	}
	if err := pw.WriteStop(); err != nil {
		return nil, false, err
	}

	// Finished reading all tracked physical addresses
	// return marker - set if we broke because of size, or completed the scan
	hasData := count > 0
	return nextMark, hasData, nil
}
