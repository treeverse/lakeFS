package catalog

import (
	"context"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

func gcWriteUncommitted(ctx context.Context, store Store, repository *graveler.RepositoryRecord, w *UncommittedWriter, mark *GCUncommittedMark, runID string, maxFileSize int64, prepareDuration time.Duration) (*GCUncommittedMark, bool, error) {
	pw, err := writer.NewParquetWriterFromWriter(w, new(UncommittedParquetObject), gcParquetParallelNum)
	if err != nil {
		return nil, false, err
	}
	pw.CompressionType = parquet.CompressionCodec_GZIP

	branchIterator, err := store.ListBranches(ctx, repository)
	if err != nil {
		return nil, false, err
	}
	defer branchIterator.Close()

	normalizedStorageNamespace := normalizeStorageNamespace(string(repository.StorageNamespace))

	if mark != nil {
		branchIterator.SeekGE(mark.BranchID)
	}

	count := 0
	var nextMark *GCUncommittedMark
	hasData := false
	startTime := time.Now()
	for branchIterator.Next() {
		nextMark, count, err = processBranch(ctx, store, repository, branchIterator.Value().BranchID, runID, pw, normalizedStorageNamespace, maxFileSize, prepareDuration, w, count, mark, startTime)
		if err != nil {
			return nil, false, err
		}
		if nextMark != nil {
			break
		}
	}
	if branchIterator.Err() != nil {
		return nil, false, branchIterator.Err()
	}

	if err := pw.WriteStop(); err != nil {
		return nil, false, err
	}

	if count > 0 {
		hasData = true
	}
	return nextMark, hasData, err
}

func normalizeStorageNamespace(namespace string) string {
	if !strings.HasSuffix(namespace, DefaultPathDelimiter) {
		namespace += DefaultPathDelimiter
	}
	return namespace
}

func processBranch(ctx context.Context, store Store, repository *graveler.RepositoryRecord, branchID graveler.BranchID, runID string, parquetWriter *writer.ParquetWriter, normalizedStorageNamespace string, maxFileSize int64, prepareDuration time.Duration, writer *UncommittedWriter, count int, mark *GCUncommittedMark, startTime time.Time) (*GCUncommittedMark, int, error) {
	diffIterator, err := store.DiffUncommitted(ctx, repository, branchID)
	if err != nil {
		return nil, 0, err
	}
	defer diffIterator.Close()

	var nextMark *GCUncommittedMark

	if mark != nil && mark.BranchID == branchID && mark.Path != "" {
		diffIterator.SeekGE(graveler.Key(mark.Path))
	}

	for diffIterator.Next() {
		diff := diffIterator.Value()

		// Skip tombstones
		if diff.Type == graveler.DiffTypeRemoved {
			continue
		}

		entry, err := ValueToEntry(diff.Value)
		if err != nil {
			return nil, 0, err
		}

		// Skip non-relative addresses outside the storage namespace
		entryAddress := entry.Address
		if entry.AddressType != Entry_RELATIVE {
			if !strings.HasPrefix(entry.Address, normalizedStorageNamespace) {
				continue
			}
			entryAddress = entryAddress[len(normalizedStorageNamespace):]
		}

		count++
		if count%gcPeriodicCheckSize == 0 {
			if err := parquetWriter.Flush(true); err != nil {
				return nil, 0, err
			}
		}

		// check if we need to stop - based on max file size or prepare duration.
		// prepare duration is optional, if 0 it will be ignored.
		// prepare duration is used to stop the process in cases we scan a lot of data, and we want to stop
		// so the api call will not time out.
		if writer.Size() > maxFileSize || (prepareDuration > 0 && time.Since(startTime) > prepareDuration) {
			nextMark = &GCUncommittedMark{
				RunID:    runID,
				BranchID: branchID,
				Path:     Path(diff.Key.String()),
			}
			break
		}

		err = parquetWriter.Write(UncommittedParquetObject{
			PhysicalAddress: entryAddress,
			CreationDate:    entry.LastModified.AsTime().Unix(),
		})
		if err != nil {
			return nil, 0, err
		}
	}

	return nextMark, count, diffIterator.Err()
}
