package s3

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"

	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/treeverse/lakefs/block"
	inventorys3 "github.com/treeverse/lakefs/inventory/s3"
	"github.com/treeverse/lakefs/logging"
)

type Manifest struct {
	URL                string          `json:"-"`
	InventoryBucketArn string          `json:"destinationBucket"`
	SourceBucket       string          `json:"sourceBucket"`
	Files              []inventoryFile `json:"files"`
	Format             string          `json:"fileFormat"`
	inventoryBucket    string
}

type inventoryFile struct {
	Key string `json:"key"`
}

func (a *Adapter) GenerateInventory(ctx context.Context, logger logging.Logger, manifestURL string, shouldSort bool) (block.Inventory, error) {
	return GenerateInventory(ctx, logger, manifestURL, a.s3, inventorys3.NewReader(ctx, a.s3, logger), shouldSort)
}

func GenerateInventory(ctx context.Context, logger logging.Logger, manifestURL string, s3 s3iface.S3API, inventoryReader inventorys3.IReader, shouldSort bool) (block.Inventory, error) {
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
	return &Inventory{Manifest: m, S3: s3, ctx: ctx, logger: logger, shouldSort: shouldSort, reader: inventoryReader}, nil
}

type Inventory struct {
	S3         s3iface.S3API
	Manifest   *Manifest
	ctx        context.Context //nolint:structcheck // known issue: https://github.com/golangci/golangci-lint/issues/826)
	logger     logging.Logger
	shouldSort bool
	reader     inventorys3.IReader
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
		return nil, err
	}
	var m Manifest
	err = json.NewDecoder(output.Body).Decode(&m)
	if err != nil {
		return nil, err
	}
	if m.Format != inventorys3.OrcFormatName && m.Format != inventorys3.ParquetFormatName {
		return nil, fmt.Errorf("%w. got format: %s", inventorys3.ErrUnsupportedInventoryFormat, m.Format)
	}
	m.URL = manifestURL
	inventoryBucketArn, err := arn.Parse(m.InventoryBucketArn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse inventory bucket arn: %w", err)
	}
	m.inventoryBucket = inventoryBucketArn.Resource
	return &m, nil
}

func sortManifest(m *Manifest, logger logging.Logger, reader inventorys3.IReader) error {
	firstKeyByInventoryFile := make(map[string]string)
	lastKeyByInventoryFile := make(map[string]string)
	for _, f := range m.Files {
		mr, err := reader.GetInventoryMetadataReader(m.Format, m.inventoryBucket, f.Key)
		if err != nil {
			return fmt.Errorf("failed to sort inventory files in manifest: %w", err)
		}
		firstKeyByInventoryFile[f.Key] = mr.MinValue()
		lastKeyByInventoryFile[f.Key] = mr.MaxValue()
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
	return nil
}
