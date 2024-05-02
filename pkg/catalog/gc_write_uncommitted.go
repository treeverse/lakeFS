package catalog

import (
	"context"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/xitongsys/parquet-go/writer"
)

func gcWriteUncommitted(ctx context.Context, store Store, repository *graveler.RepositoryRecord, w *UncommittedWriter, mark *GCUncommittedMark, runID string, maxFileSize int64, prepareDuration time.Duration) (*GCUncommittedMark, bool, error) {
	pw, err := writer.NewParquetWriterFromWriter(w, new(UncommittedParquetObject), gcParquetParallelNum)
	if err != nil {
		return nil, false, err
	}
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

	var nextMark *GCUncommittedMark
	var hasData bool
	var err error

	// process the first branch
	nextMark, hasData, err = processBranch(ctx, store, repository, branchIterator.Value().BranchID, runID, parquetWriter, normalizedStorageNamespace, maxFileSize, prepareDuration, writer)
	if nextMark != nil {
		return nextMark, hasData, err
	}

	for branchIterator.Next() {
		mark, foundData, err := processBranch(ctx, store, repository, branchIterator.Value().BranchID, runID, parquetWriter, normalizedStorageNamespace, maxFileSize, prepareDuration, writer)
		if err != nil {
			return nil, false, err
		}
		if mark != nil {
			nextMark = mark
			break
		}
		if foundData {
			hasData = true
		}
	}

	return nextMark, hasData, branchIterator.Err()
}

func processBranch(ctx context.Context, store Store, repository *graveler.RepositoryRecord, branchID graveler.BranchID, runID string, parquetWriter *writer.ParquetWriter, normalizedStorageNamespace string, maxFileSize int64, prepareDuration time.Duration, writer *UncommittedWriter) (*GCUncommittedMark, bool, error) {
	diffIterator, err := store.DiffUncommitted(ctx, repository, branchID)
	if err != nil {
		return nil, false, err
	}
	defer diffIterator.Close()

	count := 0
	startTime := time.Now()
	var nextMark *GCUncommittedMark
	hasData := false

	for diffIterator.Next() {
		diff := diffIterator.Value()

		// Skip tombstones
		if diff.Type == graveler.DiffTypeRemoved {
			continue
		}

		entry, err := ValueToEntry(diff.Value)
		if err != nil {
			return nil, false, err
		}

		// Skip non-relative addresses outside the storage namespace
		entryAddress := entry.Address
		if entry.AddressType != Entry_RELATIVE || !strings.HasPrefix(entry.Address, normalizedStorageNamespace) {
			continue
		}
		entryAddress = entryAddress[len(normalizedStorageNamespace):] // Adjust if relative

		count++
		hasData = true

		if count%gcPeriodicCheckSize == 0 {
			if err := parquetWriter.Flush(true); err != nil {
				return nil, false, err
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
			return nil, false, err
		}
	}

	return nextMark, hasData, diffIterator.Err()
}
