package onboard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	s3parquet "github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/reader"
	"net/url"
	"sort"
)

type Manifest struct {
	InventoryBucketArn string `json:"destinationBucket"`
	SourceBucket       string `json:"sourceBucket"`
	Files              []File `json:"files"`
	Format             string `json:"fileFormat"`
}

type File struct {
	Key         string `json:"key"`
	Size        int    `json:"size"`
	MD5checksum string `json:"MD5checksum"`
}

type FileRow struct {
	Error          error
	Bucket         string `parquet:"name=bucket, type=INTERVAL"`
	Key            string `parquet:"name=key, type=INTERVAL"`
	Size           *int64 `parquet:"name=size, type=INT_64"`
	ETag           string `parquet:"name=e_tag, type=INTERVAL"`
	LastModified   int64  `parquet:"name=last_modified_date, type=TIMESTAMP_MILLIS"`
	IsLatest       bool   `parquet:"name=is_latest, type=BOOLEAN"`
	IsDeleteMarker bool   `parquet:"name=is_delete_marker, type=BOOLEAN"`
}

func (s *FileRow) String() string {
	return s.Key
}

type Diff struct {
	AddedOrChanged []FileRow
	Deleted        []FileRow
}

type IInventory interface {
	LoadManifest() error
	Fetch(ctx context.Context, sorted bool) error
	Rows() []FileRow
	Manifest() *Manifest
	ManifestURL() string
}

type Inventory struct {
	s3          s3iface.S3API
	rowReader   func(ctx context.Context, svc s3iface.S3API, invBucket string, file File) ([]FileRow, error)
	rows        []FileRow
	manifestURL string
	manifest    *Manifest
}

func NewInventory(s3 s3iface.S3API, manifestURL string) IInventory {
	return &Inventory{s3: s3, manifestURL: manifestURL, rowReader: readRows}
}

func readRows(ctx context.Context, svc s3iface.S3API, invBucket string, file File) ([]FileRow, error) {
	pf, err := s3parquet.NewS3FileReaderWithClient(ctx, svc, invBucket, file.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet file reader: %v", err)
	}
	pr, err := reader.NewParquetReader(pf, new(FileRow), 4)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %v", err)
	}
	num := int(pr.GetNumRows())
	currentRows := make([]FileRow, num)
	err = pr.Read(&currentRows)
	if err != nil {
		_ = pf.Close()
		return nil, err
	}
	err = pf.Close()
	return currentRows, err
}

func CompareKeys(row1 *FileRow, row2 *FileRow) bool {
	if row1 == nil || row2 == nil {
		return false
	}
	return row1.Key < row2.Key
}

// InventoryDiff returns a diff between two sorted arrays of FileRow
func InventoryDiff(leftInv []FileRow, rightInv []FileRow) Diff {
	res := Diff{}
	var leftIdx, rightIdx int
	for leftIdx < len(leftInv) || rightIdx < len(rightInv) {
		var leftRow, rightRow *FileRow
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
	return res
}

func (i *Inventory) LoadManifest() error {
	u, err := url.Parse(i.manifestURL)
	if err != nil {
		return err
	}
	output, err := i.s3.GetObject(&s3.GetObjectInput{Bucket: &u.Host, Key: &u.Path})
	if err != nil {
		return err
	}
	err = json.NewDecoder(output.Body).Decode(&i.manifest)
	if err != nil {
		return err
	}
	if i.manifest.Format != "Parquet" {
		return errors.New("currently only parquet inventories are supported. got: " + i.manifest.Format)
	}
	return err
}

// FetchInventory reads the parquet files specified in the given manifest, and unifies them to an array of FileRow
func (i *Inventory) Fetch(ctx context.Context, sorted bool) error {
	if i.manifest == nil {
		return fmt.Errorf("manifest not loaded yet")
	}
	i.rows = nil
	inventoryBucketArn, err := arn.Parse(i.manifest.InventoryBucketArn)
	if err != nil {
		return fmt.Errorf("failed to parse inventory bucket arn: %v", err)
	}
	invBucket := inventoryBucketArn.Resource
	for _, file := range i.manifest.Files {
		currentRows, err := i.rowReader(ctx, i.s3, invBucket, file)
		if err != nil {
			return err
		}
		for _, row := range currentRows {
			if !row.IsDeleteMarker && row.IsLatest {
				i.rows = append(i.rows, row)
			}
		}
	}
	if sorted {
		sort.SliceStable(i.rows, func(i1, i2 int) bool {
			return i.rows[i1].Key < i.rows[i2].Key
		})
	}
	return nil
}

func (i *Inventory) Rows() []FileRow {
	return i.rows
}

func (i *Inventory) Manifest() *Manifest {
	return i.manifest
}

func (i *Inventory) ManifestURL() string {
	return i.manifestURL
}
