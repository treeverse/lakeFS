package s3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

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
	URL                string         `json:"-"`
	InventoryBucketArn string         `json:"destinationBucket"`
	SourceBucket       string         `json:"sourceBucket"`
	Files              []manifestFile `json:"files"`
	Format             string         `json:"fileFormat"`
	inventoryBucket    string
}

type manifestFile struct {
	Key string `json:"key"`
}

type ManifestFileReader interface {
	Read(dstInterface interface{}) error
	GetNumRows() int64
	SkipRows(int64) error
	Close() error
}

type CloseFunc func() error

var ErrUnsupportedInventoryFormat = errors.New("unsupported inventory type. supported types: parquet, orc")

func (a *Adapter) GenerateInventory(ctx context.Context, logger logging.Logger, manifestURL string) (block.Inventory, error) {
	return GenerateInventory(ctx, logger, manifestURL, a.s3)
}

func GenerateInventory(ctx context.Context, logger logging.Logger, manifestURL string, s3 s3iface.S3API) (block.Inventory, error) {
	if logger == nil {
		logger = logging.Default()
	}
	m, err := loadManifest(manifestURL, s3)
	if err != nil {
		return nil, err
	}
	return &Inventory{Manifest: m, S3: s3, ctx: ctx, logger: logger}, nil
}

type Inventory struct {
	S3       s3iface.S3API
	Manifest *Manifest
	ctx      context.Context //nolint:structcheck // known issue: https://github.com/golangci/golangci-lint/issues/826)
	logger   logging.Logger
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
	if m.Format != OrcFormatName && m.Format != ParquetFormatName {
		return nil, fmt.Errorf("%w. got format: %s", ErrUnsupportedInventoryFormat, m.Format)
	}
	m.URL = manifestURL
	inventoryBucketArn, err := arn.Parse(m.InventoryBucketArn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse inventory bucket arn: %w", err)
	}
	m.inventoryBucket = inventoryBucketArn.Resource
	return &m, nil
}
