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

	nextMark, hasData, err := processBranches(ctx, store, repository, branchIterator, pw, normalizedStorageNamespace, mark, runID, maxFileSize, prepareDuration, w)
	if err := pw.WriteStop(); err != nil {
		return nil, false, err
	}
	return nextMark, hasData, err
}

func normalizeStorageNamespace(namespace string) string {
	if !strings.HasSuffix(namespace, DefaultPathDelimiter) {
		namespace += DefaultPathDelimiter
	}
	return namespace
}

func processBranches(ctx context.Context, store Store, repository *graveler.RepositoryRecord, branchIterator graveler.BranchIterator, parquetWriter *writer.ParquetWriter, normalizedStorageNamespace string, mark *GCUncommittedMark, runID string, maxFileSize int64, prepareDuration time.Duration, writer *UncommittedWriter) (*GCUncommittedMark, bool, error) {
	if mark != nil {
		branchIterator.SeekGE(mark.BranchID)
	} else if !branchIterator.Next() {
		return nil, false, nil // No branches
	}

	count := 0
	countPtr := &count
	// process the first branch before calling Next() again
	hasData := false
	nextMark, err := processBranch(ctx, store, repository, branchIterator.Value().BranchID, runID, parquetWriter, normalizedStorageNamespace, maxFileSize, prepareDuration, writer, countPtr, mark)
	if nextMark != nil {
		return nextMark, true, err
	}
	for branchIterator.Next() {
		nextMark, err = processBranch(ctx, store, repository, branchIterator.Value().BranchID, runID, parquetWriter, normalizedStorageNamespace, maxFileSize, prepareDuration, writer, countPtr, mark)
		if err != nil {
			return nil, false, err
		}
		if nextMark != nil {
			return nextMark, true, nil
		}
	}

	if *countPtr > 0 {
		hasData = true
	}
	return nil, hasData, branchIterator.Err()
}

func processBranch(ctx context.Context, store Store, repository *graveler.RepositoryRecord, branchID graveler.BranchID, runID string, parquetWriter *writer.ParquetWriter, normalizedStorageNamespace string, maxFileSize int64, prepareDuration time.Duration, writer *UncommittedWriter, count *int, mark *GCUncommittedMark) (*GCUncommittedMark, error) {
	diffIterator, err := store.DiffUncommitted(ctx, repository, branchID)
	if err != nil {
		return nil, err
	}
	defer diffIterator.Close()

	startTime := time.Now()
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
			return nil, err
		}

		// Skip non-relative addresses outside the storage namespace
		entryAddress := entry.Address
		if entry.AddressType != Entry_RELATIVE {
			if !strings.HasPrefix(entry.Address, normalizedStorageNamespace) {
				continue
			}
			entryAddress = entryAddress[len(normalizedStorageNamespace):]
		}

		*count++

		if *count%gcPeriodicCheckSize == 0 {
			if err := parquetWriter.Flush(true); err != nil {
				return nil, err
			}
		}

		// Check for size or duration limits
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
			return nil, err
		}
	}

	return nextMark, diffIterator.Err()
}
