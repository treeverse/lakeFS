package s3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/cloud/aws/s3inventory"
	"github.com/treeverse/lakefs/pkg/logging"
)

var ErrInventoryFilesRangesOverlap = errors.New("got s3 inventory with files covering overlapping ranges")

type Manifest struct {
	URL                string          `json:"-"`
	InventoryBucketArn string          `json:"destinationBucket"`
	SourceBucket       string          `json:"sourceBucket"`
	Files              []InventoryFile `json:"files"` // inventory list files, each contains a list of objects
	Format             string          `json:"fileFormat"`
	CreationTimestamp  string          `json:"creationTimestamp"`
	inventoryBucket    string
}

type InventoryFile struct {
	Key      string `json:"key"` // an s3 key for an inventory list file
	firstKey string
	lastKey  string
}

func (a *Adapter) GenerateInventory(ctx context.Context, logger logging.Logger, manifestURL string, shouldSort bool, prefixes []string) (block.Inventory, error) {
	m, err := a.loadManifest(ctx, manifestURL)
	if err != nil {
		return nil, err
	}
	svc := a.clients.Get(ctx, m.inventoryBucket)
	return GenerateInventory(logger, m, s3inventory.NewReader(ctx, svc, logger), shouldSort, prefixes)
}

func GenerateInventory(logger logging.Logger, m *Manifest, inventoryReader s3inventory.IReader, shouldSort bool, prefixes []string) (block.Inventory, error) {
	if logger == nil {
		logger = logging.Default()
	}
	if shouldSort || len(prefixes) > 0 {
		if err := sortManifest(m, logger, inventoryReader); err != nil {
			return nil, err
		}
	}
	if len(prefixes) > 0 {
		manifestFileCount := len(m.Files)
		m.Files = filterFiles(m.Files, prefixes)
		logger.Debugf("manifest filtered from %d to %d files", manifestFileCount, len(m.Files))
	}
	return &Inventory{Manifest: m, logger: logger, shouldSort: shouldSort, reader: inventoryReader, prefixes: prefixes}, nil
}

type Inventory struct {
	Manifest   *Manifest
	logger     logging.Logger
	shouldSort bool
	reader     s3inventory.IReader
	prefixes   []string
}

func (inv *Inventory) Iterator() block.InventoryIterator {
	return NewInventoryIterator(inv)
}

func (inv *Inventory) SourceName() string {
	return inv.Manifest.SourceBucket
}

func (inv *Inventory) InventoryURL() string {
	return inv.Manifest.URL
}

func (a *Adapter) loadManifest(ctx context.Context, manifestURL string) (*Manifest, error) {
	u, err := url.Parse(manifestURL)
	if err != nil {
		return nil, err
	}
	output, err := a.clients.Get(ctx, u.Host).GetObject(&s3.GetObjectInput{Bucket: &u.Host, Key: &u.Path})
	if err != nil {
		return nil, fmt.Errorf("%w: failed to read manifest.json from %s", err, manifestURL)
	}
	var m Manifest
	err = json.NewDecoder(output.Body).Decode(&m)
	if err != nil {
		return nil, err
	}
	if m.Format != s3inventory.OrcFormatName && m.Format != s3inventory.ParquetFormatName {
		return nil, fmt.Errorf("%w. got format: %s", s3inventory.ErrUnsupportedInventoryFormat, m.Format)
	}
	m.URL = manifestURL
	inventoryBucketArn, err := arn.Parse(m.InventoryBucketArn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse inventory bucket arn: %w", err)
	}
	m.inventoryBucket = inventoryBucketArn.Resource
	return &m, nil
}

func filterFiles(files []InventoryFile, prefixes []string) []InventoryFile {
	sort.Strings(prefixes)
	currentPrefixIdx := 0
	filteredFiles := make([]InventoryFile, 0)
	for i := 0; i < len(files); i++ {
		for {
			// find a prefix that may have suitable keys in the current file
			if prefixes[currentPrefixIdx] >= files[i].firstKey {
				// prefix may be in scope of current file
				break
			}
			if strings.HasPrefix(files[i].firstKey, prefixes[currentPrefixIdx]) {
				// first object in file starts with prefix
				break
			}
			// current prefix ends before this file - move to next prefix
			currentPrefixIdx++
			if currentPrefixIdx == len(prefixes) {
				// no more prefixes - other files are irrelevant
				return filteredFiles
			}
		}
		if strings.HasPrefix(files[i].firstKey, prefixes[currentPrefixIdx]) ||
			(prefixes[currentPrefixIdx] >= files[i].firstKey && prefixes[currentPrefixIdx] < files[i].lastKey) {
			// file may contain keys starting with this prefix
			filteredFiles = append(filteredFiles, files[i])
		}
	}
	return filteredFiles
}

func sortManifest(m *Manifest, logger logging.Logger, reader s3inventory.IReader) error {
	for i, f := range m.Files {
		mr, err := reader.GetMetadataReader(m.Format, m.inventoryBucket, f.Key)
		if err != nil {
			return fmt.Errorf("failed to sort inventory files in manifest: %w", err)
		}
		m.Files[i].firstKey = mr.FirstObjectKey()
		m.Files[i].lastKey = mr.LastObjectKey()
		err = mr.Close()
		if err != nil {
			logger.WithError(err).WithField("file", f).Error("Failed to close inventory file")
		}
	}
	sort.Slice(m.Files, func(i, j int) bool {
		return m.Files[i].firstKey < m.Files[j].firstKey ||
			(m.Files[i].firstKey == m.Files[j].firstKey && m.Files[i].lastKey < m.Files[j].lastKey)
	})
	// validate sorting: if a file begins before the next one ends - the files cover overlapping ranges,
	// which we don't know how to handle.
	for i := 0; i < len(m.Files)-1; i++ {
		if m.Files[i+1].firstKey < m.Files[i].lastKey {
			return ErrInventoryFilesRangesOverlap
		}
	}
	return nil
}
