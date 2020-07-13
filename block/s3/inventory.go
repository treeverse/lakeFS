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
	"sort"
)

func (s *Adapter) GenerateInventory(manifestURL string) (block.Inventory, error) {
	manifest, err := LoadManifest(manifestURL, s.s3)
	if err != nil {
		return nil, err
	}
	return &Inventory{Manifest: manifest, S3: s.s3, RowReader: readRows}, nil
}

type Inventory struct {
	RowReader func(ctx context.Context, svc s3iface.S3API, invBucket string, file ManifestFile) ([]block.InventoryObject, error)
	S3        s3iface.S3API
	Manifest  *Manifest
}

func (i *Inventory) GetPhysicalAddress(object block.InventoryObject) string {
	return "s3://" + object.Bucket + "/" + object.Key
}

func (i *Inventory) Objects(ctx context.Context, sorted bool) (objects []block.InventoryObject, err error) {
	inventoryBucketArn, err := arn.Parse(i.Manifest.InventoryBucketArn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse inventory bucket arn: %w", err)
	}
	invBucket := inventoryBucketArn.Resource
	for _, file := range i.Manifest.Files {
		var currentRows []block.InventoryObject
		currentRows, err = i.RowReader(ctx, i.S3, invBucket, file)
		if err != nil {
			return
		}
		for _, row := range currentRows {
			if !row.IsDeleteMarker && row.IsLatest {
				row.PhysicalAddress = "s3://" + row.Bucket + "/" + row.Key
				objects = append(objects, row)
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
func LoadManifest(manifestURL string, s3svc s3iface.S3API) (manifest *Manifest, err error) {
	u, err := url.Parse(manifestURL)
	if err != nil {
		return
	}
	output, err := s3svc.GetObject(&s3.GetObjectInput{Bucket: &u.Host, Key: &u.Path})
	if err != nil {
		return
	}
	manifest = new(Manifest)
	err = json.NewDecoder(output.Body).Decode(manifest)
	if err != nil {
		return
	}
	if manifest.Format != "Parquet" {
		return nil, errors.New("currently only parquet inventories are supported. got: " + manifest.Format)
	}
	manifest.URL = manifestURL
	return
}

func readRows(ctx context.Context, svc s3iface.S3API, invBucket string, file ManifestFile) ([]block.InventoryObject, error) {
	pf, err := s3parquet.NewS3FileReaderWithClient(ctx, svc, invBucket, file.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet file reader: %w", err)
	}
	defer func() {
		_ = pf.Close()
	}()
	pr, err := reader.NewParquetReader(pf, new(block.InventoryObject), 4)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	num := int(pr.GetNumRows())
	currentRows := make([]block.InventoryObject, num)
	err = pr.Read(&currentRows)
	if err != nil {
		return nil, err
	}
	return currentRows, nil
}

type Manifest struct {
	URL                string         `json:"-"`
	InventoryBucketArn string         `json:"destinationBucket"`
	SourceBucket       string         `json:"sourceBucket"`
	Files              []ManifestFile `json:"files"`
	Format             string         `json:"fileFormat"`
}

type ManifestFile struct {
	Key         string `json:"key"`
	Size        int    `json:"size"`
	MD5checksum string `json:"MD5checksum"`
}
