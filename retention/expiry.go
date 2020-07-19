package retention

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/fileutil"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/uri"
)

// WriteExpiryResultsToSeekableReader returns a file-backed (Seeker) Reader holding the contents of expiryRows.
func WriteExpiryResultsToSeekableReader(ctx context.Context, expiryRows catalog.ExpiryRows) (io.ReadSeeker, error) {
	logger := logging.FromContext(ctx)

	writer, err := fileutil.NewFileWriterThenReader("expired_entries_*.json")
	if err != nil {
		return nil, fmt.Errorf("creating temporary storage to write expiry records: %w", err)
	}
	logger = logger.WithField("filename", file.Name())
	err = os.Remove(file.Name())
	if err != nil {
		logger.WithError(err).Warning("remove temporary file; keep going, leaving some garbage")
	}
	encoder := json.NewEncoder(file)
	count := 0
	for ; expiryRows.Next(); count++ {
		expiry, err := expiryRows.Read()
		if err != nil {
			logger.WithField("record_number", count).WithError(err).Warning("failed to read record; keep going, lose this expiry")
		}
		err = encoder.Encode(expiry)
		if err != nil {
			logger.WithFields(logging.Fields{"record_number": count, "record": expiry}).WithError(err).Warning("failed to write record; keep going, lose this expiry")
		}
	}
	length, err := file.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, fmt.Errorf("seeking to start of file holding expiry records: %w", err)
	}
	logger.WithFields(logging.Fields{"length": length, "num_records": count}).Info("wrote expiry records")
	return file, nil
}

// storageNamespaceEntry holds a URI or an error for the physical location of a storage
// namespace.
type storageNamespaceEntry struct {
	resolvedNamespace block.QualifiedKey
	err               error
}

// storageNamespaceCache caches repository namespace information from the catalog.
//
// BUG(ariels): This access is not transactional.  After a repository is deleted or modified it
//     becomes impossible to expire its objects.  (Unclear what _should_ happen.)
type storageNamespaceCache struct {
	c catalog.Cataloger
	m map[string]storageNamespaceEntry
}

func newStorageNamespaceCache(c catalog.Cataloger) storageNamespaceCache {
	return storageNamespaceCache{c, make(map[string]storageNamespaceEntry)}
}

func (cache *storageNamespaceCache) get(ctx context.Context, repository string) (block.QualifiedKey, error) {
	entry, ok := cache.m[repository]
	if ok {
		return entry.resolvedNamespace, entry.err
	}
	repositoryEntry, err := cache.c.GetRepository(ctx, repository)
	if err == nil {
		entry.resolvedNamespace, entry.err = uri.Parse(repositoryEntry.StorageNamespace)
	}
	cache.m[repository] = entry
	return entry.resolvedNamespace, entry.err
}

// WriteExpiryManifestFromSeekableReader reads from r ExpiryResults and writes to w a CSV
// suitable for passing to AWS S3 batch tagging.
func WriteExpiryManifestFromReader(ctx context.Context, c catalog.Cataloger, r io.Reader, w io.Writer) error {
	logger := logging.FromContext(ctx)
	cache := newStorageNamespaceCache(c)
	decoder := json.NewDecoder(r)
	encoder := csv.NewWriter(w)
	count := 0
	var err error
	for ; ; count++ {
		record := catalog.ExpireResult{}
		err = decoder.Decode(&record)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			logger.WithField("record_number", count).WithError(err).
				Warning("failed to read record; keep going, lose this expiry")
			continue
		}
		resolvedNamespace, err := cache.get(ctx, record.Repository)
		if err != nil {
			logger.WithFields(logging.Fields{"record_number": count, "record": record}).
				WithError(err).Warning("failed to get repository URI; keep going, lose this expiry")
			continue
		}
		if resolvedNamespace.StorageType != resolvedNamespace.StorageType.S3 {
			logger.WithFields(logging.Fields{
				"record_number":      count,
				"record":             record,
				"resolved_namespace": resolvedNamespace,
			}).
				WithError(err).
				Warning("cannot expire on repository which is not on S3; keep going, lose this expiry")
			continue
		}
		objectKey := fmt.Sprintf("%s/%s", resolvedNamespace.Key, record.PhysicalAddress)
		err = encoder.Write([]string{resolvedNamespace.StorageNamespace, objectKey})
		if err != nil {
			logger.WithFields(logging.Fields{
				"record_number":      count,
				"record":             record,
				"resolved_namespace": resolvedNamespace,
			}).
				WithError(err).
				Warning("cannot expire on repository which is not on S3; keep going, lose this expiry")
			continue
		}
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}
