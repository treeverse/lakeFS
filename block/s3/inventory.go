package s3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/treeverse/lakefs/block"
	s3parquet "github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/reader"
	"net/url"
)

const DefaultReadBatchSize = 1000

type manifest struct {
	URL                string         `json:"-"`
	InventoryBucketArn string         `json:"destinationBucket"`
	SourceBucket       string         `json:"sourceBucket"`
	Files              []manifestFile `json:"files"`
	Format             string         `json:"fileFormat"`
}

type manifestFile struct {
	Key         string `json:"key"`
	Size        int    `json:"size"`
	MD5checksum string `json:"MD5checksum"`
}

type ParquetReader interface {
	Read(dstInterface interface{}) error
	GetNumRows() int64
}

type ParquetInventoryObject struct {
	Bucket         string  `parquet:"name=bucket, type=UTF8"`
	Key            string  `parquet:"name=key, type=UTF8"`
	IsLatest       *bool   `parquet:"name=is_latest, type=BOOLEAN"`
	IsDeleteMarker *bool   `parquet:"name=is_delete_marker, type=BOOLEAN"`
	Size           *int64  `parquet:"name=size, type=INT_64"`
	LastModified   *int64  `parquet:"name=last_modified_date, type=TIMESTAMP_MILLIS"`
	Checksum       *string `parquet:"name=e_tag, type=UTF8"`
}

type parquetReaderGetter func(ctx context.Context, svc s3iface.S3API, invBucket string, manifestFileKey string) (ParquetReader, error)

func (o *ParquetInventoryObject) GetPhysicalAddress() string {
	return "s3://" + o.Bucket + "/" + o.Key
}

func (s *Adapter) GenerateInventory(manifestURL string) (block.Inventory, error) {
	return GenerateInventory(manifestURL, s.s3)
}

func GenerateInventory(manifestURL string, s3 s3iface.S3API) (block.Inventory, error) {
	manifest, err := loadManifest(manifestURL, s3)
	if err != nil {
		return nil, err
	}
	return &Inventory{Manifest: manifest, S3: s3, GetParquetReader: getParquetReader}, nil
}

type Inventory struct {
	S3               s3iface.S3API
	Manifest         *manifest
	GetParquetReader parquetReaderGetter
}

func (i *Inventory) Iterator(ctx context.Context) (block.InventoryIterator, error) {
	inventoryBucketArn, err := arn.Parse(i.Manifest.InventoryBucketArn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse inventory bucket arn: %w", err)
	}
	invBucket := inventoryBucketArn.Resource
	return &InventoryIterator{
		s3:                     i.S3,
		ctx:                    ctx,
		manifest:               *i.Manifest,
		inventoryBucket:        invBucket,
		currentManifestFileIdx: -1,
		getParquetReader:       i.GetParquetReader,
	}, nil
}

func (i *Inventory) SourceName() string {
	return i.Manifest.SourceBucket
}

func (i *Inventory) InventoryURL() string {
	return i.Manifest.URL
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
		return nil, errors.New("currently only parquet inventories are supported. got: " + m.Format)
	}
	m.URL = manifestURL
	return &m, nil
}

func getParquetReader(ctx context.Context, svc s3iface.S3API, invBucket string, manifestFileKey string) (ParquetReader, error) {
	pf, err := s3parquet.NewS3FileReaderWithClient(ctx, svc, invBucket, manifestFileKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet file reader: %w", err)
	}
	defer func() {
		_ = pf.Close()
	}()
	var rawObject ParquetInventoryObject
	pr, err := reader.NewParquetReader(pf, &rawObject, 4)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	return pr, nil
}
