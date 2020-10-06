package s3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"

	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/cloud/aws/s3inventory"
	"github.com/treeverse/lakefs/logging"
)

var ErrInventoryFilesRangesOverlap = errors.New("got s3 inventory with files covering overlapping ranges")

type Manifest struct {
	URL                string          `json:"-"`
	InventoryBucketArn string          `json:"destinationBucket"`
	SourceBucket       string          `json:"sourceBucket"`
	Files              []inventoryFile `json:"files"` // inventory list files, each contains a list of objects
	Format             string          `json:"fileFormat"`
	CreationTimestamp  string          `json:"creationTimestamp"`
	inventoryBucket    string
}

type inventoryFile struct {
	Key string `json:"key"` // an s3 key for an inventory list file
}

func (a *Adapter) GenerateInventory(ctx context.Context, logger logging.Logger, manifestURL string, shouldSort bool) (block.Inventory, error) {
	return GenerateInventory(logger, manifestURL, a.s3, s3inventory.NewReader(ctx, a.s3, logger), shouldSort)
}

func GenerateInventory(logger logging.Logger, manifestURL string, s3 s3iface.S3API, inventoryReader s3inventory.IReader, shouldSort bool) (block.Inventory, error) {
	if logger == nil {
		logger = logging.Default()
	}
	m, err := loadManifest(manifestURL, s3)
	if err != nil {
		return nil, err
	}
	if shouldSort {
		err = sortManifest(m, logger, inventoryReader)
	}
	if err != nil {
		return nil, err
	}
	return &Inventory{Manifest: m, logger: logger, shouldSort: shouldSort, reader: inventoryReader}, nil
}

type Inventory struct {
	Manifest   *Manifest
	logger     logging.Logger
	shouldSort bool
	reader     s3inventory.IReader
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

func loadManifest(manifestURL string, s3svc s3iface.S3API) (*Manifest, error) {
	u, err := url.Parse(manifestURL)
	if err != nil {
		return nil, err
	}
	output, err := s3svc.GetObject(&s3.GetObjectInput{Bucket: &u.Host, Key: &u.Path})
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

func sortManifest(m *Manifest, logger logging.Logger, reader s3inventory.IReader) error {
	firstKeyByInventoryFile := make(map[string]string)
	lastKeyByInventoryFile := make(map[string]string)
	for _, f := range m.Files {
		mr, err := reader.GetMetadataReader(m.Format, m.inventoryBucket, f.Key)
		if err != nil {
			return fmt.Errorf("failed to sort inventory files in manifest: %w", err)
		}
		firstKeyByInventoryFile[f.Key] = mr.FirstObjectKey()
		lastKeyByInventoryFile[f.Key] = mr.LastObjectKey()
		err = mr.Close()
		if err != nil {
			logger.Errorf("failed to close inventory file. file=%s, err=%w", f, err)
		}
	}
	sort.Slice(m.Files, func(i, j int) bool {
		return firstKeyByInventoryFile[m.Files[i].Key] < firstKeyByInventoryFile[m.Files[j].Key] ||
			(firstKeyByInventoryFile[m.Files[i].Key] == firstKeyByInventoryFile[m.Files[j].Key] &&
				lastKeyByInventoryFile[m.Files[i].Key] < lastKeyByInventoryFile[m.Files[j].Key])
	})
	// validate sorting: if a file begins before the next one ends - the files cover overlapping ranges,
	// which we don't know how to handle.
	for i := 0; i < len(m.Files)-1; i++ {
		if firstKeyByInventoryFile[m.Files[i+1].Key] < lastKeyByInventoryFile[m.Files[i].Key] {
			return ErrInventoryFilesRangesOverlap
		}
	}
	return nil
}
