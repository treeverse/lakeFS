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
	s3parquet "github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/reader"
)

type manifest struct {
	URL                string         `json:"-"`
	InventoryBucketArn string         `json:"destinationBucket"`
	SourceBucket       string         `json:"sourceBucket"`
	Files              []manifestFile `json:"files"`
	Format             string         `json:"fileFormat"`
}

type manifestFile struct {
	Key string `json:"key"`
}

type ParquetReader interface {
	Read(dstInterface interface{}) error
	GetNumRows() int64
	SkipRows(int64) error
}

type parquetReaderGetter func(ctx context.Context, svc s3iface.S3API, invBucket string, manifestFileKey string) (ParquetReader, CloseFunc, error)

type CloseFunc func() error

var (
	ErrParquetOnlySupport = errors.New("currently only parquet inventories are supported")
)

func (s *Adapter) GenerateInventory(logger logging.Logger, manifestURL string) (block.Inventory, error) {
	return GenerateInventory(logger, manifestURL, s.s3, getParquetReader)
}

func GenerateInventory(logger logging.Logger, manifestURL string, s3 s3iface.S3API, getParquetReader parquetReaderGetter) (block.Inventory, error) {
	manifest, err := loadManifest(manifestURL, s3)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = logging.Default()
	}
	return &Inventory{Manifest: manifest, S3: s3, getParquetReader: getParquetReader, logger: logger}, nil
}

type Inventory struct {
	S3               s3iface.S3API
	Manifest         *manifest
	getParquetReader parquetReaderGetter
	logger           logging.Logger
}

func (inv *Inventory) Iterator(ctx context.Context) (block.InventoryIterator, error) {
	inventoryBucketArn, err := arn.Parse(inv.Manifest.InventoryBucketArn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse inventory bucket arn: %w", err)
	}
	invBucket := inventoryBucketArn.Resource
	return NewInventoryIterator(ctx, inv, invBucket)
}

func (inv *Inventory) SourceName() string {
	return inv.Manifest.SourceBucket
}

func (inv *Inventory) InventoryURL() string {
	return inv.Manifest.URL
}

func loadManifest(manifestURL string, s3svc s3iface.S3API) (*manifest, error) {
	u, err := url.Parse(manifestURL)
	if err != nil {
		return nil, err
	}
	output, err := s3svc.GetObject(&s3.GetObjectInput{Bucket: &u.Host, Key: &u.Path})
	if err != nil {
		return nil, err
	}
	var m manifest
	err = json.NewDecoder(output.Body).Decode(&m)
	if err != nil {
		return nil, err
	}
	if m.Format != "Parquet" {
		return nil, fmt.Errorf("%w. got: %s", ErrParquetOnlySupport, m.Format)
	}
	m.URL = manifestURL
	return &m, nil
}

func getParquetReader(ctx context.Context, svc s3iface.S3API, invBucket string, manifestFileKey string) (ParquetReader, CloseFunc, error) {
	pf, err := s3parquet.NewS3FileReaderWithClient(ctx, svc, invBucket, manifestFileKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create parquet file reader: %w", err)
	}
	var rawObject ParquetInventoryObject
	pr, err := reader.NewParquetReader(pf, &rawObject, 4)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	closer := func() error {
		pr.ReadStop()
		return pf.Close()
	}
	return pr, closer, nil
}
