package onboard

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/treeverse/lakefs/catalog"
	s3parquet "github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/reader"
	"sort"
	"strconv"
	"time"
)

func CompareKeys(row1 *InventoryObject, row2 *InventoryObject) bool {
	if row1 == nil || row2 == nil {
		return false
	}
	return row1.Key < row2.Key
}

type InventoryDiff struct {
	DryRun               bool
	AddedOrChanged       []InventoryObject
	Deleted              []InventoryObject
	PreviousInventoryURL string
	PreviousImportDate   time.Time
}

// CalcDiff returns a diff between two sorted arrays of InventoryObject
func CalcDiff(leftInv []InventoryObject, rightInv []InventoryObject) *InventoryDiff {
	res := InventoryDiff{}
	var leftIdx, rightIdx int
	for leftIdx < len(leftInv) || rightIdx < len(rightInv) {
		var leftRow, rightRow *InventoryObject
		if leftIdx < len(leftInv) {
			leftRow = &leftInv[leftIdx]
		}
		if rightIdx < len(rightInv) {
			rightRow = &rightInv[rightIdx]
		}
		if leftRow != nil && rightRow == nil || CompareKeys(leftRow, rightRow) {
			res.Deleted = append(res.Deleted, *leftRow)
			leftIdx++
		} else if leftRow == nil || CompareKeys(rightRow, leftRow) {
			res.AddedOrChanged = append(res.AddedOrChanged, *rightRow)
			rightIdx++
		} else if leftRow.Key == rightRow.Key {
			if leftRow.ETag != rightRow.ETag {
				res.AddedOrChanged = append(res.AddedOrChanged, *rightRow)
			}
			leftIdx++
			rightIdx++
		}
	}
	return &res
}

// IInventory represents all objects referenced by a single manifest
type Inventory interface {
	Fetch(ctx context.Context, sorted bool) error
	Objects() []InventoryObject
	SourceName() string
	CreateCommitMetadata(diff InventoryDiff) catalog.Metadata
}

type InventoryFactory interface {
	NewInventory(inventoryURL string) (Inventory, error)
}

type S3InventoryFactory struct {
	s3 s3iface.S3API
}

func (s *S3InventoryFactory) NewInventory(manifestURL string) (Inventory, error) {
	manifest, err := LoadManifest(manifestURL, s.s3)
	if err != nil {
		return nil, err
	}
	return &S3Inventory{S3: s.s3, manifest: manifest, RowReader: readRows}, nil
}

func NewS3InventoryFactory(s3 s3iface.S3API) *S3InventoryFactory {
	return &S3InventoryFactory{s3: s3}
}

type S3Inventory struct {
	S3        s3iface.S3API
	RowReader func(ctx context.Context, svc s3iface.S3API, invBucket string, file ManifestFile) ([]InventoryObject, error)
	objects   []InventoryObject
	manifest  *Manifest
}

type InventoryObject struct {
	Error          error
	Bucket         string `parquet:"name=bucket, type=INTERVAL"`
	Key            string `parquet:"name=key, type=INTERVAL"`
	Size           *int64 `parquet:"name=size, type=INT_64"`
	ETag           string `parquet:"name=e_tag, type=INTERVAL"`
	LastModified   int64  `parquet:"name=last_modified_date, type=TIMESTAMP_MILLIS"`
	IsLatest       bool   `parquet:"name=is_latest, type=BOOLEAN"`
	IsDeleteMarker bool   `parquet:"name=is_delete_marker, type=BOOLEAN"`
}

func (s *InventoryObject) String() string {
	return s.Key
}

func readRows(ctx context.Context, svc s3iface.S3API, invBucket string, file ManifestFile) ([]InventoryObject, error) {
	pf, err := s3parquet.NewS3FileReaderWithClient(ctx, svc, invBucket, file.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet file reader: %v", err)
	}
	pr, err := reader.NewParquetReader(pf, new(InventoryObject), 4)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %v", err)
	}
	num := int(pr.GetNumRows())
	currentRows := make([]InventoryObject, num)
	err = pr.Read(&currentRows)
	if err != nil {
		_ = pf.Close()
		return nil, err
	}
	err = pf.Close()
	return currentRows, err
}

// FetchInventory reads the parquet files specified in the given manifest, and unifies them to an array of InventoryObject
func (i *S3Inventory) Fetch(ctx context.Context, sorted bool) error {
	i.objects = nil
	inventoryBucketArn, err := arn.Parse(i.manifest.InventoryBucketArn)
	if err != nil {
		return fmt.Errorf("failed to parse inventory bucket arn: %v", err)
	}
	invBucket := inventoryBucketArn.Resource
	for _, file := range i.manifest.Files {
		currentRows, err := i.RowReader(ctx, i.S3, invBucket, file)
		if err != nil {
			return err
		}
		for _, row := range currentRows {
			if !row.IsDeleteMarker && row.IsLatest {
				i.objects = append(i.objects, row)
			}
		}
	}
	if sorted {
		sort.SliceStable(i.objects, func(i1, i2 int) bool {
			return i.objects[i1].Key < i.objects[i2].Key
		})
	}
	return nil
}

func (i *S3Inventory) Objects() []InventoryObject {
	return i.objects
}

func (i *S3Inventory) SourceName() string {
	return i.manifest.SourceBucket
}

func (i *S3Inventory) CreateCommitMetadata(diff InventoryDiff) catalog.Metadata {
	return catalog.Metadata{
		"manifest_url":             i.manifest.URL,
		"source_bucket":            i.manifest.SourceBucket,
		"added_or_changed_objects": strconv.Itoa(len(diff.AddedOrChanged)),
		"deleted_objects":          strconv.Itoa(len(diff.Deleted)),
	}
}

func ExtractInventoryURL(metadata catalog.Metadata) string {
	return metadata["manifest_url"]
}
