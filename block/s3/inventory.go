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
	"github.com/treeverse/lakefs/logging"
)

const (
	OrcFormatName     = "ORC"
	ParquetFormatName = "Parquet"
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

type InventoryMetadataReader interface {
	GetNumRows() int64
	SkipRows(int64) error
	Close() error
	MinValue() string
	MaxValue() string
}

type InventoryFileReader interface {
	InventoryMetadataReader
	Read(dstInterface interface{}) error
}

type CloseFunc func() error

var ErrUnsupportedInventoryFormat = errors.New("unsupported inventory type. supported types: parquet, orc")

func (a *Adapter) GenerateInventory(ctx context.Context, logger logging.Logger, manifestURL string, shouldSort bool) (block.Inventory, error) {
	return GenerateInventory(ctx, logger, manifestURL, a.s3, shouldSort)
}

func GenerateInventory(ctx context.Context, logger logging.Logger, manifestURL string, s3 s3iface.S3API, shouldSort bool) (block.Inventory, error) {
	if logger == nil {
		logger = logging.Default()
	}
	m, err := loadManifest(ctx, manifestURL, s3, logger, shouldSort)
	if err != nil {
		return nil, err
	}
	return &Inventory{Manifest: m, S3: s3, ctx: ctx, logger: logger}, nil
}

type Inventory struct {
	S3         s3iface.S3API
	Manifest   *Manifest
	ctx        context.Context //nolint:structcheck // known issue: https://github.com/golangci/golangci-lint/issues/826)
	logger     logging.Logger
	shouldSort bool
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

func loadManifest(ctx context.Context, manifestURL string, s3svc s3iface.S3API, logger logging.Logger, shouldSort bool) (*Manifest, error) {
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
	if m.Format != OrcFormatName && m.Format != ParquetFormatName {
		return nil, fmt.Errorf("%w. got format: %s", ErrUnsupportedInventoryFormat, m.Format)
	}
	m.URL = manifestURL
	inventoryBucketArn, err := arn.Parse(m.InventoryBucketArn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse inventory bucket arn: %w", err)
	}
	m.inventoryBucket = inventoryBucketArn.Resource
	if !shouldSort {
		return &m, nil
	}
	reader := NewInventoryReader(ctx, s3svc, &m, logger)
	firstKeyByInventoryFile := make(map[string]string)
	lastKeyByInventoryFile := make(map[string]string)
	for _, f := range m.Files {
		mr, err := reader.GetInventoryMetadataReader(f.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to sort inventory files in manifest: %w", err)
		}
		err = mr.Close()
		if err != nil {
			logger.Errorf("failed to close inventory file. file=%s, err=%w", f, err)
		}
		firstKeyByInventoryFile[f.Key] = mr.MinValue()
		lastKeyByInventoryFile[f.Key] = mr.MaxValue()
	}
	sort.Slice(m.Files, func(i, j int) bool {
		return firstKeyByInventoryFile[m.Files[i].Key] < firstKeyByInventoryFile[m.Files[j].Key] ||
			(firstKeyByInventoryFile[m.Files[i].Key] == firstKeyByInventoryFile[m.Files[j].Key] &&
				lastKeyByInventoryFile[m.Files[i].Key] < lastKeyByInventoryFile[m.Files[j].Key])
	})
	return &m, nil
}
