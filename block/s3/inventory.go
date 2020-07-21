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
	Key         string `json:"key"`
	Size        int    `json:"size"`
	MD5checksum string `json:"MD5checksum"`
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
	return &Inventory{Manifest: manifest, S3: s3, RowReader: readRows}, nil
}

type Inventory struct {
	RowReader func(ctx context.Context, svc s3iface.S3API, invBucket string, manifestFileKey string) ([]ParquetInventoryObject, error)
	S3        s3iface.S3API
	Manifest  *manifest
}

func (i *Inventory) Objects(ctx context.Context, sorted bool) (objects []block.InventoryObject, err error) {
	inventoryBucketArn, err := arn.Parse(i.Manifest.InventoryBucketArn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse inventory bucket arn: %w", err)
	}
	invBucket := inventoryBucketArn.Resource
	for _, file := range i.Manifest.Files {
		err = ctx.Err()
		if err != nil {
			return
		}
		var currentRows []ParquetInventoryObject
		currentRows, err = i.RowReader(ctx, i.S3, invBucket, file.Key)
		if err != nil {
			return
		}
		for _, row := range currentRows {
			isDeleteMarker := false
			isLatest := true
			if row.IsDeleteMarker != nil {
				isDeleteMarker = *row.IsDeleteMarker
			}
			if row.IsLatest != nil {
				isLatest = *row.IsLatest
			}
			if !isDeleteMarker && isLatest {
				o := block.InventoryObject{
					Bucket:          row.Bucket,
					Key:             row.Key,
					PhysicalAddress: row.GetPhysicalAddress(),
				}
				if row.Size != nil {
					o.Size = *row.Size
				}
				if row.LastModified != nil {
					o.LastModified = *row.LastModified
				}
				if row.Checksum != nil {
					o.Checksum = *row.Checksum
				}
				objects = append(objects, o)
			}
		}
	}
	if sorted {
		sort.SliceStable(objects, func(i1, i2 int) bool {
			return objects[i1].Key < objects[i2].Key
		})
	}
	return
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

func readRows(ctx context.Context, svc s3iface.S3API, invBucket string, manifestFileKey string) ([]ParquetInventoryObject, error) {
	pf, err := s3parquet.NewS3FileReaderWithClient(ctx, svc, invBucket, manifestFileKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet file reader: %w", err)
	}
	defer func() {
		_ = pf.Close()
	}()
	var rawObject ParquetInventoryObject
	pr, err := reader.NewParquetReader(pf, rawObject, 4)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	num := int(pr.GetNumRows())
	rawInventoryObjects := make([]ParquetInventoryObject, num)
	err = pr.Read(&rawInventoryObjects)
	if err != nil {
		return nil, err
	}
	return rawInventoryObjects, nil
}
