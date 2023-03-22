package catalog

import (
	"context"
	"strings"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

func gcWriteUncommitted(ctx context.Context, store Store, repository *graveler.RepositoryRecord, w *UncommittedWriter, mark *GCUncommittedMark, runID string, maxFileSize int64) (*GCUncommittedMark, bool, error) {
	pw, err := writer.NewParquetWriterFromWriter(w, new(UncommittedParquetObject), gcParquetParallelNum)
	if err != nil {
		return nil, false, err
	}
	pw.CompressionType = parquet.CompressionCodec_GZIP

	// write uncommitted data from branches
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
	var nextMark *GCUncommittedMark
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
			nextMark = &GCUncommittedMark{
				RunID:    runID,
				BranchID: entry.branchID,
				Path:     entry.Path,
			}
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
	// stop writer before we return
	if err := pw.WriteStop(); err != nil {
		return nil, false, err
	}

	// Finished reading all staging area - return marker to switch processing tracked physical addresses
	hasData := count > 0
	return nextMark, hasData, nil
}
